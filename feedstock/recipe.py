### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import apache_beam as beam
from dataclasses import dataclass
from typing import List
from pangeo_forge_esgf import get_urls_from_esgf, setup_logging
from pangeo_forge_big_query.utils import BQInterface, LogToBigQuery
from pangeo_forge_esgf.parsing import parse_instance_ids
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec, OpenWithXarray, StoreToZarr, Indexed, T
)
import asyncio
import xarray as xr
import zarr

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
    


iids_raw = [
    # from https://github.com/Timh37/CMIP6cex/issues/2
    'CMIP6.*.*.*.historical.*.day.psl.*.*',
    'CMIP6.*.*.*.historical.*.day.sfcWind.*.*',
    'CMIP6.*.*.*.historical.*.day.pr.*.*',
    'CMIP6.*.*.*.ssp245.*.day.psl.*.*',
    'CMIP6.*.*.*.ssp245.*.day.sfcWind.*.*',
    'CMIP6.*.*.*.ssp245.*.day.pr.*.*',
    'CMIP6.*.*.*.ssp585.*.day.psl.*.*',
    'CMIP6.*.*.*.ssp585.*.day.sfcWind.*.*',
    'CMIP6.*.*.*.ssp585.*.day.pr.*.*',
    # from https://github.com/pangeo-forge/cmip6-feedstock/issues/22
    'CMIP6.*.*.*.historical.*.Omon.zmeso.*.*'
    'CMIP6.*.*.*.ssp126.*.Omon.zmeso.*.*',
    'CMIP6.*.*.*.ssp245.*.Omon.zmeso.*.*',
    'CMIP6.*.*.*.ssp585.*.Omon.zmeso.*.*',
    # PMIP velocities
    'CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.uo.gn.v20191002',
    'CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Odec.uo.gn.v20200212',
    'CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Omon.uo.gn.v20200212',
    'CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.uo.gr1.v20200911',
    'CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.lgm.r1i1p1f1.Omon.uo.gn.v20200909',
    'CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Omon.vo.gn.v20200212',
    'CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.vo.gn.v20191002',
    'CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Odec.vo.gn.v20200212',
    'CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.vo.gr1.v20200911',
    'CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.lgm.r1i1p1f1.Omon.vo.gn.v20190710',
    # from https://github.com/leap-stc/cmip6-leap-feedstock/issues/41
    'CMIP6.*.*.*.historical.*.SImon.sifb.*.*',
    'CMIP6.*.*.*.ssp126.*.SImon.sifb.*.*',
    'CMIP6.*.*.*.ssp245.*.SImon.sifb.*.*',
    'CMIP6.*.*.*.ssp585.*.SImon.sifb.*.*',
    'CMIP6.*.*.*.historical.*.SImon.siitdthick.*.*',
    'CMIP6.*.*.*.ssp126.*.SImon.siitdthick.*.*',
    'CMIP6.*.*.*.ssp245.*.SImon.siitdthick.*.*',
    'CMIP6.*.*.*.ssp585.*.SImon.siitdthick.*.*',
    # for CMIP6 pco2 testbed
    "CMIP6.*.*.*.historical.*.Omon.spco2.*.*",
    "CMIP6.*.*.*.historical.*.Omon.tos.*.*",
    "CMIP6.*.*.*.historical.*.Omon.sos.*.*",
    "CMIP6.*.*.*.historical.*.Omon.chl.*.*",
    "CMIP6.*.*.*.historical.*.Omon.mlotst.*.*",
    "CMIP6.*.*.*.ssp245.*.Omon.spco2.*.*",
    "CMIP6.*.*.*.ssp245.*.Omon.tos.*.*",
    "CMIP6.*.*.*.ssp245.*.Omon.sos.*.*",
    "CMIP6.*.*.*.ssp245.*.Omon.chl.*.*",
    "CMIP6.*.*.*.ssp245.*.Omon.mlotst.*.*",
]

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


prune_iids = False
prune_submission = False # if set, only submits a subset of the iids in the final step

# parse out wildcard iids using pangeo-forge-esgf
iids = parse_wildcards(iids_raw)

print(iids)

# exclude dupes
iids = list(set(iids))

# Prune the url dict to only include items that have not been logged to BQ yet
print("Pruning iids that already exist")
table_id = 'leap-pangeo.testcmip6.cmip6_feedstock_test2'
table_id_nonqc = 'leap-pangeo.testcmip6.cmip6_feedstock_test2_nonqc'
table_id_legacy = "leap-pangeo.testcmip6.cmip6_legacy"
# TODO: To create a non-QC catalog I need to find the difference between the two tables iids

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

## Create the recipes
target_chunks_aspect_ratio = {'time': 1}
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
            target_chunk_size='150MB',
            target_chunks_aspect_ratio = target_chunks_aspect_ratio,
            size_tolerance=0.5,
            allow_fallback_algo=True,
            )
        | "Logging to non-QC table" >> LogToBigQuery(iid=iid, table_id=table_id_nonqc)
        | TestDataset(iid=iid)
        | "Logging to QC table" >> LogToBigQuery(iid=iid, table_id=table_id)
        )
