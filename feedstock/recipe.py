### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import apache_beam as beam
from typing import Dict
from dask.utils import parse_bytes
from pangeo_forge_esgf import setup_logging
from leap_data_management_utils import CMIPBQInterface, LogCMIPToBigQuery
from leap_data_management_utils.data_management_transforms import Copy, InjectAttrs
from leap_data_management_utils.cmip_transforms import TestDataset, Preprocessor
from pangeo_forge_esgf.client import ESGFClient
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    ConsolidateMetadata,
    ConsolidateDimensionCoordinates,
)
import logging
import os
import xarray as xr
import yaml
from tqdm.auto import tqdm

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

client = ESGFClient(
    file_output_fields=[
        "pid",
        "tracking_id",
        "further_info_url",
        "citation_url",
        "checksum",
        "checksum_type",
    ],
    dataset_output_fields=["pid", "tracking_id", "further_info_url", "citation_url"],
)
iid_info_dict = client.get_instance_id_input(iids_raw)
iids = list(iid_info_dict.keys())
logger.info(f"{iids = }")

# Prune the url dict to only include items that have not been logged to BQ yet
logger.info("Pruning iids that already exist")
bq_interface = CMIPBQInterface(table_id=table_id)

# TODO: Move this back to the BQ client https://github.com/leap-stc/leap-data-management-utils/issues/33
# Since we have more than 10k iids to check against the big query database,
# we need to run this in batches (bq does not take more than 10k inputs per query).
iids_in_table = []
batchsize = 10000
iid_batches = [iids[i : i + batchsize] for i in range(0, len(iids), batchsize)]
for iids_batch in tqdm(iid_batches):
    iids_in_table_batch = bq_interface.iid_list_exists(iids_batch)
    iids_in_table.extend(iids_in_table_batch)

# manual overrides (these will be rewritten each time as long as they exist here)
overwrite_iids = [
    # "CMIP6.HighResMIP.MOHC.HadGEM3-GC31-HH.highres-future.r1i1p1f1.Omon.thetao.gn.v20200514",
    # "CMIP6.HighResMIP.NERC.HadGEM3-GC31-HH.hist-1950.r1i1p1f1.Omon.thetao.gn.v20200514",
    # "CMIP6.HighResMIP.MOHC.HadGEM3-GC31-HH.highres-future.r1i1p1f1.Omon.so.gn.v20200514",
    # "CMIP6.HighResMIP.NERC.HadGEM3-GC31-HH.hist-1950.r1i1p1f1.Omon.so.gn.v20200514",
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


# Now that we have the iids that are not yet ingested, we can prune the full iid_info_dict and extract the 'id' field
iid_info_dict_filtered = {k: v for k, v in iid_info_dict.items() if k in iids_filtered}
dataset_ids_filtered = [v["id"] for v in iid_info_dict_filtered.values()]

print(f"🚀 Requesting a total of {len(dataset_ids_filtered)} datasets")
input_dict = client.get_recipe_inputs_from_dataset_ids(dataset_ids_filtered)

logger.debug(f"{input_dict=}")
input_dict_flat = {
    iid: [(k, v) for k, v in data.items()] for iid, data in input_dict.items()
}
logger.debug(f"{input_dict_flat=}")


def combine_dicts(dicts):
    result = {}
    for d in dicts:
        for key, value in d.items():
            if key in result:
                result[key].append(value)
            else:
                result[key] = [value]
    return result


recipe_dict = {
    iid: combine_dicts([i[1] for i in sorted(data)])
    for iid, data in input_dict_flat.items()
}
logger.debug(f"{recipe_dict=}")

if prune_submission:
    recipe_dict = {iid: [recipe_dict[iid] for iid in list(recipe_dict.keys())[0:10]]}

print(f"🚀 Submitting a total of {len(recipe_dict)} iids")

# Print the actual urls
logger.debug(f"{recipe_dict = }")


## Dynamic Chunking Wrapper
def dynamic_chunking_func(ds: xr.Dataset) -> Dict[str, int]:
    import warnings

    # trying to import inside the function
    from dynamic_chunks.algorithms import (
        even_divisor_algo,
        iterative_ratio_increase_algo,
        NoMatchingChunks,
    )

    logger.info(f"Input Dataset for dynamic chunking {ds =}")

    target_chunk_size = "150MB"
    target_chunks_aspect_ratio = {
        "time": 10,
        "x": 1,
        "i": 1,
        "ni": 1,
        "xh": 1,
        "nlon": 1,
        "lon": 1,  # TODO: Maybe import all the known spatial dimensions from xmip?
        "y": 1,
        "j": 1,
        "nj": 1,
        "yh": 1,
        "nlat": 1,
        "lat": 1,
    }
    size_tolerance = 0.5

    # Some datasets are smaller than the target chunk size and should not be chunked at all
    if ds.nbytes < parse_bytes(target_chunk_size):
        target_chunks = dict(ds.dims)

    else:
        try:
            target_chunks = even_divisor_algo(
                ds,
                target_chunk_size,
                target_chunks_aspect_ratio,
                size_tolerance,
                allow_extra_dims=True,
            )

        except NoMatchingChunks:
            warnings.warn(
                "Primary algorithm using even divisors along each dimension failed "
                "with. Trying secondary algorithm."
                f"Input {ds=}"
            )
            try:
                target_chunks = iterative_ratio_increase_algo(
                    ds,
                    target_chunk_size,
                    target_chunks_aspect_ratio,
                    size_tolerance,
                    allow_extra_dims=True,
                )
            except NoMatchingChunks:
                raise ValueError(
                    (
                        "Could not find any chunk combinations satisfying "
                        "the size constraint with either algorithm."
                        f"Input {ds=}"
                    )
                )
            # If something fails
            except Exception as e:
                raise e
        except Exception as e:
            raise e
    logger.info(f"Dynamic Chunking determined {target_chunks =}")
    return target_chunks


## Create the recipes
recipes = {}

for iid, data in recipe_dict.items():
    urls = data["url"]
    pattern = pattern_from_file_sequence(urls, concat_dim="time")
    recipes[iid] = (
        f"Creating {iid}" >> beam.Create(pattern.items())
        | OpenURLWithFSSpec()
        # do not specify file type to accomodate both ncdf3 and ncdf4
        | OpenWithXarray(xarray_open_kwargs={"use_cftime": True})
        | Preprocessor()
        | StoreToZarr(
            store_name=f"{iid}.zarr",
            combine_dims=pattern.combine_dim_keys,
            dynamic_chunking_fn=dynamic_chunking_func,
        )
        | InjectAttrs({"pangeo_forge_file_data": data})
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
