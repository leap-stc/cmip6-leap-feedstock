### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import apache_beam as beam
from apache_beam.io.gcp import gcsio
from dataclasses import dataclass
from typing import List, Dict
from pangeo_forge_esgf import get_urls_from_esgf, setup_logging
from leap_data_management_utils import IIDEntry, CMIPBQInterface, LogCMIPToBigQuery
from leap_data_management_utils.cmip_testing import test_all
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

    def _test(self, store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        test_all(store, self.iid)
        return store
    
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return (pcoll
            | "Testing - Running all tests" >> beam.Map(self._test)
        )

@dataclass
class Copy(beam.PTransform):
    target_prefix: str
    
    def _copy(self,store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        # We do need the gs:// prefix? 
        # TODO: Determine this dynamically from zarr.storage.FSStore
        source = f"gs://{os.path.normpath(store.path)}/" #FIXME more elegant. `.copytree` needs trailing slash
        target = os.path.join(*[self.target_prefix]+source.split('/')[-3:])
        # gcs = gcsio.GcsIO()
        # gcs.copytree(source, target)
        print(f"HERE: Copying {source} to {target}")
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        fs.cp(source, target, recursive=True)
        # return a new store with the new path that behaves exactly like the input 
        # to this stage (so we can slot this stage right before testing/logging stages)
        return zarr.storage.FSStore(target)
        
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return (pcoll
            | "Copying Store" >> beam.Map(self._copy)
        )
    
## Create recipes
is_test = os.environ['IS_TEST'] == 'true' # There must be a better way to do this, but for now this will do
print(f"{is_test =}")

if is_test:
    setup_logging('DEBUG')
    # copy_target_bucket = "gs://leap-scratch/data-library/cmip6-pr-copied/
    copy_target_bucket ="gs://cmip6/cmip6-pgf-ingestion-test/zarr_stores_pr/" #TODO: Change this back once we are ready to release this.
    iid_file = "feedstock/iids_pr.yaml"
    prune_iids = True
    prune_submission = True # if set, only submits a subset of the iids in the final step
    table_id = 'leap-pangeo.testcmip6.cmip6_consolidated_testing_pr'
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
    setup_logging('INFO')
    copy_target_bucket = "gs://cmip6/cmip6-pgf-ingestion-test/zarr_stores/"
    iid_file = 'feedstock/iids.yaml'
    prune_iids = False
    prune_submission = False # if set, only submits a subset of the iids in the final step
    #TODO: rename these more cleanly when we move to the official bucket
    table_id = 'leap-pangeo.testcmip6.cmip6_consolidated_manual_testing' # TODO rename to `leap-pangeo.cmip6_pgf_ingestion.cmip6`
    print(f"{table_id = } {prune_submission = } {iid_file = }")

print('Running with the following parameters:')
print(f"{copy_target_bucket = }")
print(f"{iid_file = }")
print(f"{prune_iids = }")
print(f"{prune_submission = }")
print(f"{table_id = }")

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

bq_interface = CMIPBQInterface(table_id=table_id)

# get lists of the iids already logged
iids_in_table = bq_interface.iid_list_exists(iids)

# manual overrides (these will be rewritten each time as long as they exist here)
overwrite_iids = [
    "CMIP6.CMIP.NCC.NorESM2-LM.historical.r3i1p1f1.SImon.siitdthick.gn.v20191108",	
    "CMIP6.ScenarioMIP.NASA-GISS.GISS-E2-1-G-CC.ssp245.r102i1p1f1.Omon.mlotst.gn.v20220115",	
    "CMIP6.CMIP.NCC.NorESM2-LM.historical.r1i1p1f1.SImon.siitdthick.gn.v20191108",	
    "CMIP6.CMIP.NCC.NorESM2-MM.historical.r1i1p1f1.SImon.siitdthick.gn.v20191108",	
    "CMIP6.ScenarioMIP.NASA-GISS.GISS-E2-1-G-CC.ssp245.r102i1p1f1.Omon.spco2.gn.v20220115",
    "CMIP6.ScenarioMIP.NCAR.CESM2-FV2.ssp585.r1i2p2f1.SImon.siitdthick.gn.v20220915",
    "CMIP6.CMIP.NCC.NorESM2-MM.historical.r1i1p1f1.SImon.sifb.gn.v20191108",
    "CMIP6.ScenarioMIP.NASA-GISS.GISS-E2-1-G-CC.ssp245.r102i1p1f1.Omon.chl.gn.v20220115",
    # "CMIP6.HighResMIP.MOHC.HadGEM3-GC31-HH.highres-future.r1i1p1f1.Omon.thetao.gn.v20200514",
    # "CMIP6.HighResMIP.NERC.HadGEM3-GC31-HH.hist-1950.r1i1p1f1.Omon.thetao.gn.v20200514",
    # "CMIP6.HighResMIP.MOHC.HadGEM3-GC31-HH.highres-future.r1i1p1f1.Omon.so.gn.v20200514",
    # "CMIP6.HighResMIP.NERC.HadGEM3-GC31-HH.hist-1950.r1i1p1f1.Omon.so.gn.v20200514",
]
#deactivate high res runs until we find out what is going on there.

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

print(f"Running a total of {len(iids_filtered)} iids")

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
print(f"{url_dict = }")

## Dynamic Chunking Wrapper
def dynamic_chunking_func(ds: xr.Dataset) -> Dict[str, int]:
    import warnings
    # trying to import inside the function
    from dynamic_chunks.algorithms import even_divisor_algo, iterative_ratio_increase_algo, NoMatchingChunks
    
    target_chunk_size='150MB'
    target_chunks_aspect_ratio = {
        'time':20,
        'x':1, 'i':1, 'ni':1, 'xh':1, 'nlon':1, 'lon':1, # TODO: Maybe import all the known spatial dimensions from xmip?
        'y':1, 'j':1, 'nj':1, 'yh':1, 'nlat':1, 'lat':1,
    }
    size_tolerance=0.5

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
    target_prefix = f'{copy_target_bucket}' 
    print(f"{target_prefix = }")
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
        | Copy(target_prefix=copy_target_bucket)
        | "Logging to bigquery (non-QC)" >> LogCMIPToBigQuery(iid=iid, table_id=table_id, tests_passed=False)
        | TestDataset(iid=iid)
        | "Logging to bigquery (QC)" >> LogCMIPToBigQuery(iid=iid, table_id=table_id, tests_passed=True)
        )
