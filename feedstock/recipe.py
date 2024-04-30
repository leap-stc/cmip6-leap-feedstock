### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import apache_beam as beam
from typing import List, Dict
from dask.utils import parse_bytes
from pangeo_forge_esgf import get_urls_from_esgf, setup_logging
from leap_data_management_utils import CMIPBQInterface, LogCMIPToBigQuery
from leap_data_management_utils.data_management_transforms import Copy, InjectAttrs
from leap_data_management_utils.cmip_transforms import TestDataset, Preprocessor
from pangeo_forge_esgf.parsing import parse_instance_ids
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    ConsolidateMetadata,
    ConsolidateDimensionCoordinates,
)
import asyncio
import logging
import os
import xarray as xr
import yaml

logger = logging.getLogger(__name__)

## Create recipes
is_test = (
    os.environ["IS_TEST"] == "true"
)  # There must be a better way to do this, but for now this will do
print(f"{is_test =}")

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


def parse_wildcards(iids: List[str]) -> List[str]:
    """iterate through each list element and
    if it contains wilcards apply the wildcard parser
    """
    iids_parsed = []
    for iid in iids:
        if "*" in iid:
            iids_parsed += parse_instance_ids(iid)
        else:
            iids_parsed.append(iid)
    return iids_parsed


# parse out wildcard iids using pangeo-forge-esgf
print(f"{iids_raw = }")
iids = parse_wildcards(iids_raw)
print(f"{iids = }")

# exclude dupes
iids = list(set(iids))

# Prune the url dict to only include items that have not been logged to BQ yet
print("Pruning iids that already exist")

bq_interface = CMIPBQInterface(table_id=table_id)

# get lists of the iids already logged
iids_in_table = bq_interface.iid_list_exists(iids)

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
print(f"{overwrite_iids =}")
iids_to_skip = set(iids_in_table) - set(overwrite_iids)
print(f"{iids_to_skip =}")
iids_filtered = list(set(iids) - iids_to_skip)
print(f"Pruned {len(iids) - len(iids_filtered)}/{len(iids)} iids from input list")


if prune_iids:
    iids_filtered = iids_filtered[0:200]

print(f"🚀 Requesting a total of {len(iids_filtered)} iids")

# Get the urls from ESGF at Runtime (only for the pruned list to save time)
url_dict = asyncio.run(
    get_urls_from_esgf(
        iids_filtered,
        limit_per_host=20,
        max_concurrency=20,
        max_concurrency_response=20,
    )
)

if prune_submission:
    url_dict = {iid: url_dict[iid] for iid in list(url_dict.keys())[0:10]}

print(f"🚀 Submitting a total of {len(url_dict)} iids")

# Print the actual urls
print(f"{url_dict = }")


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

for iid, urls in url_dict.items():
    pattern = pattern_from_file_sequence(urls, concat_dim="time")
    recipes[iid] = (
        f"Creating {iid}" >> beam.Create(pattern.items())
        | OpenURLWithFSSpec()
        # do not specify file type to accomodate both ncdf3 and ncdf4
        | OpenWithXarray(xarray_open_kwargs={"use_cftime": True})
        | Preprocessor()
        | StoreToZarr(
            store_name=,
            combine_dims=pattern.combine_dim_keys,
            dynamic_chunking_fn=dynamic_chunking_func,
        )
        | InjectAttrs()
        | ConsolidateDimensionCoordinates()
        | ConsolidateMetadata()
        | Copy(target=os.path.join(copy_target_prefix, f"{iid}.zarr"))
        | "Logging to bigquery (non-QC)"
        >> LogCMIPToBigQuery(iid=iid, table_id=table_id, tests_passed=False)
        | TestDataset(iid=iid)
        | "Logging to bigquery (QC)"
        >> LogCMIPToBigQuery(iid=iid, table_id=table_id, tests_passed=True)
    )
