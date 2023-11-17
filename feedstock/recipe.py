### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import apache_beam as beam
from dataclasses import dataclass
from typing import List, Dict
from pangeo_forge_esgf import get_urls_from_esgf, setup_logging
from pangeo_forge_big_query.utils import BQInterface, LogToBigQuery
from pangeo_forge_esgf.parsing import parse_instance_ids
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec, OpenWithXarray, StoreToZarr, Indexed, T
)
import asyncio
import os
import xarray as xr
import yaml
import zarr
import warnings

# setup_logging('DEBUG')
setup_logging('INFO')

    
# Custom Beam Transforms

@dataclass
class Preprocessor(beam.PTransform):
    """
    Preprocessor for xarray datasets.
    Set all data_variables except for `variable_id` attrs to coord
    Add additional information 

    """

    @staticmethod
    def _keep_only_variable_id(item: Indexed[T]) -> Indexed[T]:
        """
        Many netcdfs contain variables other than the one specified in the `variable_id` facet. 
        Set them all to coords
        """
        index, ds = item
        print(f"Preprocessing before {ds =}")
        new_coords_vars = [var for var in ds.data_vars if var != ds.attrs['variable_id']]
        ds = ds.set_coords(new_coords_vars)
        print(f"Preprocessing after {ds =}")
        return index, ds
    
    @staticmethod
    def _sanitize_attrs(item: Indexed[T]) -> Indexed[T]:
        """Removes non-ascii characters from attributes see https://github.com/pangeo-forge/pangeo-forge-recipes/issues/586"""
        index, ds = item
        for att, att_value in ds.attrs.items():
            if isinstance(att_value, str):
                new_value=att_value.encode("utf-8", 'ignore').decode()
                if new_value != att_value:
                    print(f"Sanitized datasets attributes field {att}: \n {att_value} \n ----> \n {new_value}")
                    ds.attrs[att] = new_value
        return index, ds
  
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return ( pcoll 
            | "Fix coordinates" >> beam.Map(self._keep_only_variable_id)
            | "Sanitize Attrs" >> beam.Map(self._sanitize_attrs)
        )

@dataclass
class TestDataset(beam.PTransform):
    """
    Test stage for data written to zarr store
    """
    iid: str
    
    @staticmethod
    def _get_dataset(store: zarr.storage.FSStore) -> xr.Dataset:
        import xarray as xr
        return xr.open_dataset(store, engine='zarr', chunks={}, use_cftime=True)
       
    def _test_open_store(self, store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        """Can the store be opened?"""
        print(f"Written path: {store.path =}")
        ds = self._get_dataset(store)
        print(ds)
        return store
        
    def _test_time(self, store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        """
        Check time dimension
        For now checks:
        - That time increases strictly monotonically
        - That no large gaps in time (e.g. missing file) are present
        """
        ds = self._get_dataset(store)
        time_diff = ds.time.diff('time').astype(int)
        # assert that time increases monotonically
        print(time_diff)
        assert (time_diff > 0).all()
        
        # assert that there are no large time gaps
        mean_time_diff = time_diff.mean()
        normalized_time_diff = abs((time_diff - mean_time_diff)/mean_time_diff)
        assert (normalized_time_diff<0.05).all()
        return store
    
    def _test_attributes(self, store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        ds = self._get_dataset(store)
        
        # check completeness of attributes 
        iid_schema = "mip_era.activity_id.institution_id.source_id.experiment_id.variant_label.table_id.variable_id.grid_label.version"
        for facet_value, facet in zip(self.iid.split('.'), iid_schema.split('.')):
            if not 'version' in facet: #(TODO: Why is the version not in all datasets?)
                print(f"Checking {facet = } in dataset attributes")
                assert ds.attrs[facet] == facet_value
          
        return store
    
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return (pcoll
            | "Testing - Open Store" >> beam.Map(self._test_open_store)
            | "Testing - Attributes" >> beam.Map(self._test_attributes)
            | "Testing - Time Dimension" >> beam.Map(self._test_time)
        )


## Create recipes
table_id_legacy = "leap-pangeo.testcmip6.cmip6_legacy"
is_pr = os.environ['IS_PR']

if is_pr:
    iid_file = "feedstock/iids_pr.yaml"
    prune_iids = False
    prune_submission = True # if set, only submits a subset of the iids in the final step
    table_id = 'leap-pangeo.testcmip6.cmip6_feedstock_pr'
    table_id_nonqc = 'leap-pangeo.testcmip6.cmip6_feedstock_pr_nonqc'
    table_id_legacy = "leap-pangeo.testcmip6.cmip6_legacy_pr"
    #TODO: Clear out both tables before running?
    print(f"{table_id = } {table_id_nonqc = } {prune_submission = } {iid_file = }")

    ## make sure the tables are deleted before running so we can run the same iids over and over again
    ## TODO: this could be integtrated in the BQInterface class
    from google.cloud import bigquery
    client = bigquery.Client()
    for table in [table_id, table_id_nonqc, table_id_legacy]:
        client.delete_table(table, not_found_ok=True)  # Make an API request.
        print("Deleted table '{}'.".format(table))

else:
    iid_file = 'feedstock/iids.yaml'
    prune_iids = False
    prune_submission = False # if set, only submits a subset of the iids in the final step
    #TODO: rename these more cleanly when we move to the official bucket
    table_id = 'leap-pangeo.testcmip6.cmip6_feedstock_test2'
    table_id_nonqc = 'leap-pangeo.testcmip6.cmip6_feedstock_test2_nonqc'
    # TODO: To create a non-QC catalog I need to find the difference between the two tables iids
    table_id_legacy = "leap-pangeo.testcmip6.cmip6_legacy"

# load iids from file
with open(iid_file) as f:
    iids_raw = yaml.safe_load(f)
    iids_raw = [iid for iid in iids_raw if iid]

def parse_wildcards(iids:List[str]) -> List[str]:
    """iterate through each list element and 
    if it contains wilcards apply the wildcard parser
    """
    iids_parsed = []
    for iid in iids:
        if "*" in iid:
            iids_parsed +=parse_instance_ids(iid)
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

bq_interface = BQInterface(table_id=table_id)
bq_interface_nonqc = BQInterface(table_id=table_id_nonqc)
bq_interface_legacy = BQInterface(table_id=table_id_legacy)

# get lists of the iids already logged
iids_in_table = bq_interface.iid_list_exists(iids)
iids_in_table_nonqc = bq_interface_nonqc.iid_list_exists(iids)
iids_in_table_legacy = bq_interface_legacy.iid_list_exists(iids)

# beam does NOT like to pickle client objects (https://github.com/googleapis/google-cloud-python/issues/3191#issuecomment-289151187)
del bq_interface 
del bq_interface_nonqc
del bq_interface_legacy

# Maybe I want a more finegrained check here at some point, but for now this will prevent logged iids from rerunning
iids_to_skip = set(iids_in_table + iids_in_table_nonqc + iids_in_table_legacy)
iids_filtered = list(set(iids) - iids_to_skip)
print(f"Pruned {len(iids) - len(iids_filtered)}/{len(iids)} iids from input list")
print(f"Running a total of {len(iids_filtered)} iids")

if prune_iids:
    iids_filtered = iids_filtered[0:10]

# Get the urls from ESGF at Runtime (only for the pruned list to save time)
url_dict = asyncio.run(
    get_urls_from_esgf(
        iids_filtered,        
        limit_per_host=20,
        max_concurrency=20,
        max_concurrency_response = 20,
    )
)

if prune_submission:
    url_dict = {iid: url_dict[iid] for iid in list(url_dict.keys())[0:10]}

# Print the actual urls
print(url_dict)

## Dynamic Chunking Wrapper
def dynamic_chunking_func(ds: xr.Dataset) -> Dict[str, int]:
    # trying to import inside the function
    from dynamic_chunks.algorithms import even_divisor_algo, iterative_ratio_increase_algo, NoMatchingChunks
    
    target_chunk_size='150MB'
    target_chunks_aspect_ratio = {'time': 1}
    size_tolerance=0.5

    try:
        target_chunks = even_divisor_algo(
            ds,
            target_chunk_size,
            target_chunks_aspect_ratio,
            size_tolerance,
        )

    except NoMatchingChunks:
        warnings.warn(
            "Primary algorithm using even divisors along each dimension failed "
            f"with {e}. Trying secondary algorithm."
        )
        try:
            target_chunks = iterative_ratio_increase_algo(
                ds,
                target_chunk_size,
                target_chunks_aspect_ratio,
                size_tolerance,
            )
        except NoMatchingChunks:
            raise ValueError(
                (
                    "Could not find any chunk combinations satisfying "
                    "the size constraint with either algorithm."
                )
            )
        # If something fails 
        except Exception as e:
            raise e
    except Exception as e:
        raise e
    
    return target_chunks 

## Create the recipes
recipes = {}

for iid, urls in url_dict.items():
    pattern = pattern_from_file_sequence(
        urls,
        concat_dim='time'
        )
    recipes[iid] = (
        f"Creating {iid}" >> beam.Create(pattern.items())
        | OpenURLWithFSSpec()
         # do not specify file type to accomodate both ncdf3 and ncdf4
        | OpenWithXarray(xarray_open_kwargs={"use_cftime":True})
        | Preprocessor()
        | StoreToZarr(
            store_name=f"{iid}.zarr",
            combine_dims=pattern.combine_dim_keys,
            dynamic_chunking_fn=dynamic_chunking_func,
            )
        | "Logging to non-QC table" >> LogToBigQuery(iid=iid, table_id=table_id_nonqc)
        | TestDataset(iid=iid)
        | "Logging to QC table" >> LogToBigQuery(iid=iid, table_id=table_id)
        )
