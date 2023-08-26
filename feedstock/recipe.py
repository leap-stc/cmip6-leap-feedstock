### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import apache_beam as beam
from dataclasses import dataclass
from datetime import datetime
from typing import List
import json
from pangeo_forge_esgf import get_urls_from_esgf
from pangeo_forge_recipes.patterns import pattern_from_file_sequence, FilePattern
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec, OpenWithXarray, StoreToZarr, Indexed, T
)
import asyncio
import xarray as xr
import zarr


# from bigquery_interface import BQInterface, IIDEntry
## copy and paste of bigquery_interface.py until I can import 
# it properly (see https://github.com/pangeo-forge/pangeo-forge-runner/issues/92)

from google.cloud import bigquery
from typing import Optional
from google.api_core.exceptions import NotFound
import datetime

@dataclass
class IIDEntry:
    """Single row/entry for an iid
    :param iid: CMIP6 instance id
    :param store: URL to zarr store
    """
    iid: str
    store: str #TODO: should this allow other objects?

    # Check if the iid conforms to a schema
    def __post_init__(self):
        
        schema = 'mip_era.activity_id.institution_id.source_id.experiment_id.member_id.table_id.variable_id.grid_label.version'
        facets = self.iid.split('.')
        if len(facets) != len(schema.split('.')):
            raise ValueError(f'IID does not conform to CMIP6 {schema =}. Got {self.iid =}')
        #TODO: Check each facet with the controlled CMIP vocabulary

        #TODO Check store validity?
    

@dataclass
class IIDResult:
    """Class to handle the results pertaining to a single IID. 
    """
    results: bigquery.table.RowIterator
    iid: str
    
    def __post_init__(self):
        if self.results.total_rows > 0:
            self.exists=True
            self.rows = [r for r in self.results]
            self.latest_row = self.rows[0]
        else:
            self.exists=False
            


@dataclass
class BQInterface:
    """Class to read/write information from BigQuery table
    :param table_id: BigQuery table ID
    :param client: BigQuery client object
    :param result_limit: Maximum number of results to return from query
    """
    table_id: str
    client: Optional[bigquery.client.Client] = None
    result_limit: Optional[int] = 10


    def __post_init__(self):
        # TODO how do I handle the schema? This class could be used for any table, but for 
        # TODO this specific case I want to prescribe the schema
        # for now just hardcode it
        self.schema = [
            bigquery.SchemaField("instance_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("store", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        ]
        if self.client is None:
            self.client = bigquery.Client()
        
        # check if table exists, otherwise create it
        try:
            self.table = self.client.get_table(self.table_id)
        except NotFound:
            self.table = self.create_table()
    
    def create_table(self) -> bigquery.table.Table:
        """Create the table if it does not exist"""
        print(f'Creating {self.table_id =}')
        table = bigquery.Table(self.table_id, schema=self.schema)
        table = self.client.create_table(table)  # Make an API request.
        return table
    
    def insert(self, IID_entry):
        """Insert a row into the table for a given IID_entry object"""
        # Generate a timestamp to add to a bigquery row
        timestamp = datetime.datetime.now().isoformat()
        json_row = {'instance_id': IID_entry.iid, 'store': IID_entry.store, 'timestamp': timestamp}
        errors = self.client.insert_rows_json(self.table_id, [json_row])
        if errors:
            raise RuntimeError(f'Error inserting row: {errors}')
        
    def _get_query_job(self, iid:str) -> bigquery.job.query.QueryJob:
        """Get result object corresponding to a given iid"""
        # keep this in case I ever need the row index again...
        # query = f"""
        # WITH table_with_index AS (SELECT *, ROW_NUMBER() OVER ()-1 as row_index FROM `{self.table_id}`)
        # SELECT *
        # FROM `table_with_index`
        # WHERE instance_id='{iid}'
        # """
        query = f"""
        SELECT *
        FROM `{self.table_id}`
        WHERE instance_id='{iid}'
        ORDER BY timestamp DESC
        LIMIT {self.result_limit}
        """
        return self.client.query(query)
    
    def _get_iid_results(self, iid: str) -> IIDResult:
        results = self._get_query_job(iid).result() # TODO: `.result()` is waiting for the query. Should I do this here?
        return IIDResult(results, iid)
    
    def iid_exists(self, iid:str) -> bool:
        """Check if iid exists in the table"""
        return self._get_iid_results(iid).exists

# wrapper functions (not sure if this works instead of the repeated copy and paste in the transform below)
def log_to_bq(iid: str, store: zarr.storage.FSStore, table_id: str):
    bq_interface = BQInterface(table_id=table_id)
    iid_entry = IIDEntry(iid=iid, store=store.path)
    bq_interface.insert(iid_entry)
    
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
    
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return ( pcoll 
            | "Fix coordinates" >> beam.Map(self._keep_only_variable_id) 
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
    
@dataclass
class LogToBigQuery(beam.PTransform):
    """
    Logging stage for data written to zarr store
    """
    iid: str
    table_id: str

    def _log_to_bigquery(self, store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        log_to_bq(self.iid, store, self.table_id)
        return store

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return (pcoll
            | beam.Map(self._log_to_bigquery)
        )

# starting an example with a small part of https://github.com/pangeo-forge/cmip6-feedstock/issues/20
iids = [
    'CMIP6.CMIP.THU.CIESM.piControl.r1i1p1f1.Omon.uo.gn.v20200220',
 'CMIP6.CMIP.THU.CIESM.piControl.r1i1p1f1.Omon.vo.gn.v20200220',
 'CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1-HR.historical.r1i1p1f2.Omon.so.gn.v20191021',
 'CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1-HR.historical.r1i1p1f2.Omon.hfds.gn.v20191021',
 'CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1-HR.historical.r1i1p1f2.Omon.wfo.gn.v20191021',
]

# Prune the url dict to only include items that have not been logged to BQ yet
print("Pruning iids that already exist")
table_id = 'leap-pangeo.testcmip6.cmip6_feedstock_test2'
table_id_nonqc = 'leap-pangeo.testcmip6.cmip6_feedstock_test2_nonqc'
# TODO: To create a non-QC catalog I need to find the difference between the two tables iids

bq_interface = BQInterface(table_id=table_id)
bq_interface_nonqc = BQInterface(table_id=table_id_nonqc)

iids_pruned = []
for iid in iids:
    # ignore iids that are either in the qc or nonqc table (should maybe be an option later)
    if not (bq_interface.iid_exists(iid) or bq_interface_nonqc.iid_exists(iid)):
        # TODO: Print which tabe the iid is in
        iids_pruned.append(iid)
    else:
        print(f"{iid =} already exists in {table_id =} or {table_id_nonqc =}")
print(f"Pruned {len(iids) - len(iids_pruned)} iids from input list")
print(f"Running a total of {len(iids_pruned)} iids")

# beam does NOT like to pickle client objects (https://github.com/googleapis/google-cloud-python/issues/3191#issuecomment-289151187)
del bq_interface 
del bq_interface_nonqc

# Get the urls from ESGF at Runtime (only for the pruned list to save time)
url_dict = asyncio.run(get_urls_from_esgf(iids_pruned))

## Create the recipes
target_chunks_aspect_ratio = {'time': 1}
recipes = {}

for iid, urls in url_dict.items():
    pattern = pattern_from_file_sequence(
        urls,
        concat_dim='time'
        )
    recipes[iid] = (
        beam.Create(pattern.items())
        | OpenURLWithFSSpec(max_concurrency=5)
        | OpenWithXarray(xarray_open_kwargs={"use_cftime":True}) # do not specify file type to accomodate both ncdf3 and ncdf4
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
