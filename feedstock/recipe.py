### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import apache_beam as beam
from pangeo_forge_esgf import setup_logging
from leap_data_management_utils.data_management_transforms import Copy, InjectAttrs
from leap_data_management_utils.cmip_transforms import (
    TestDataset,
    Preprocessor,
    dynamic_chunking_func,
    CMIPBQInterface,
    LogCMIPToBigQuery,
)
from pangeo_forge_esgf.async_client import (
    ESGFAsyncClient,
    get_sorted_http_urls_from_iid_dict,
)
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    ConsolidateMetadata,
    ConsolidateDimensionCoordinates,
    # CheckpointFileTransfer,
)
from pangeo_forge_recipes.storage import CacheFSSpecTarget

import logging
import asyncio
import os
import yaml
import gcsfs

logger = logging.getLogger(__name__)

## Create recipes
is_test = (
    os.environ["IS_TEST"] == "true"
)  # There must be a better way to do this, but for now this will do
print(f"{is_test =}")

run_id = os.environ["GITHUB_RUN_ID"]
run_attempt = os.environ["GITHUB_RUN_ATTEMPT"]

if is_test:
    setup_logging("DEBUG")
    copy_target_prefix = "gs://leap-scratch/data-library/cmip6-pr-copied/"
    iid_file = "feedstock/iids_pr.yaml"
    prune_iids = True
    prune_submission = (
        True  # if set, only submits a subset of the iids in the final step
    )
    table_id = "leap-pangeo.testcmip6.cmip6_consolidated_testing_pr"
    print(f"{table_id = } {prune_submission = } {iid_file = }")

    ## make sure the tables are deleted before running so we can run the same iids over and over again
    ## TODO: this could be integtrated in the CMIPBQInterface class
    from google.cloud import bigquery

    client = bigquery.Client()
    for table in [table_id]:
        client.delete_table(table, not_found_ok=True)  # Make an API request.
        print("Deleted table '{}'.".format(table))
    del client

else:
    setup_logging("INFO")
    copy_target_prefix = "gs://cmip6/cmip6-pgf-ingestion-test/zarr_stores/"
    iid_file = "feedstock/iids.yaml"
    prune_iids = False
    prune_submission = (
        False  # if set, only submits a subset of the iids in the final step
    )
    # TODO: rename these more cleanly when we move to the official bucket
    table_id = "leap-pangeo.testcmip6.cmip6_consolidated_manual_testing"  # TODO rename to `leap-pangeo.cmip6_pgf_ingestion.cmip6`
    print(f"{table_id = } {prune_submission = } {iid_file = }")

print("Running with the following parameters:")
print(f"{copy_target_prefix = }")
print(f"{iid_file = }")
print(f"{prune_iids = }")
print(f"{prune_submission = }")
print(f"{table_id = }")

# load iids from file
with open(iid_file) as f:
    iids_raw = yaml.safe_load(f)
    iids_raw = [iid for iid in iids_raw if iid]

# parse out wildcard/square brackets using pangeo-forge-esgf
logger.debug(f"{iids_raw = }")


async def parse_iids():
    async with ESGFAsyncClient() as client:
        return await client.expand_iids(iids_raw)


iids = asyncio.run(parse_iids())
logger.info(f"Submitted {iids = }")

# Prune the url dict to only include items that have not been logged to BQ yet
logger.info("Pruning iids that already exist")
bq_interface = CMIPBQInterface(table_id=table_id)

# TODO: Move this back to the BQ client https://github.com/leap-stc/leap-data-management-utils/issues/33
# Since we have more than 10k iids to check against the big query database,
# we need to run this in batches (bq does not take more than 10k inputs per query).
iids_in_table = bq_interface.iid_list_exists(iids)

# manual overrides (these will be rewritten each time as long as they exist here)
overwrite_iids = [
    # 'CMIP6.HighResMIP.NERC.HadGEM3-GC31-HH.hist-1950.r1i1p1f1.Omon.thetao.gn.v20200514',
    #  'CMIP6.HighResMIP.NERC.HadGEM3-GC31-HH.hist-1950.r1i1p1f1.Omon.so.gn.v20200514'
]

# beam does NOT like to pickle client objects (https://github.com/googleapis/google-cloud-python/issues/3191#issuecomment-289151187)
del bq_interface

# Maybe I want a more finegrained check here at some point, but for now this will prevent logged iids from rerunning
logger.debug(f"{overwrite_iids =}")
iids_to_skip = set(iids_in_table) - set(overwrite_iids)
logger.debug(f"{iids_to_skip =}")
iids_filtered = list(set(iids) - iids_to_skip)
logger.info(f"Pruned {len(iids) - len(iids_filtered)}/{len(iids)} iids from input list")

if prune_iids:
    iids_filtered = iids_filtered[0:20]

print(f"🚀 Requesting a total of {len(iids_filtered)} datasets")


async def get_recipe_inputs():
    async with ESGFAsyncClient() as client:
        return await client.recipe_data(iids_filtered)


recipe_data = asyncio.run(get_recipe_inputs())
logger.info(f"Got urls for iids: {list(recipe_data.keys())}")

if prune_submission:
    recipe_data = {i: recipe_data[i] for i in list(recipe_data.keys())[0:5]}

logger.info(f"🚀 Submitting a total of {len(recipe_data)} iids")

# Print the actual data
logger.debug(f"{recipe_data=}")

## Create the recipes
recipes = {}

cache_target = CacheFSSpecTarget(
    fs=gcsfs.GCSFileSystem(),
    root_path="gs://leap-scratch/data-library/cmip6-pgf-ingestion/cache",
)

for iid, data in recipe_data.items():
    urls = get_sorted_http_urls_from_iid_dict(data)
    pattern = pattern_from_file_sequence(urls, concat_dim="time")

    # to accomodate single file we cannot parse target chunks (https://github.com/pangeo-forge/pangeo-forge-recipes/issues/275)
    if len(urls) > 1:
        chunk_fn = dynamic_chunking_func
        combine_dims = pattern.combine_dim_keys
    else:
        chunk_fn = None
        combine_dims = []

    recipes[iid] = (
        f"Creating {iid}" >> beam.Create(pattern.items())
        # | CheckpointFileTransfer(
        #     transfer_target=cache_target,
        #     max_executors=10,
        #     concurrency_per_executor=4,
        #     initial_backoff=3.0,  # Try with super long backoff and
        #     backoff_factor=2.0,
        #     fsspec_sync_patch=False,
        # )
        | OpenURLWithFSSpec(
            cache=cache_target,
            # fsspec_sync_patch=True
        )
        | OpenWithXarray(xarray_open_kwargs={"use_cftime": True})
        | Preprocessor()
        | StoreToZarr(
            store_name=f"{iid}.zarr",
            combine_dims=combine_dims,
            dynamic_chunking_fn=chunk_fn,
        )
        | InjectAttrs({"pangeo_forge_api_responses": data})
        | ConsolidateDimensionCoordinates()
        | ConsolidateMetadata()
        | Copy(
            target=os.path.join(
                copy_target_prefix, f"{run_id}_{run_attempt}", f"{iid}.zarr"
            )
        )
        | "Logging to bigquery (non-QC)"
        >> LogCMIPToBigQuery(iid=iid, table_id=table_id, tests_passed=False)
        | TestDataset(iid=iid)
        | "Logging to bigquery (QC)"
        >> LogCMIPToBigQuery(iid=iid, table_id=table_id, tests_passed=True)
    )
