from google.cloud import bigquery
from dataclasses import dataclass
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
    
    def does_iid_exist(self, iid:str) -> bool:
        """Check if iid exists in the table"""
        return self._get_iid_results(iid).exists



    # outline
    # This class should be able to:
    # 0. Create a bq client if none is provided
    # 1. Check if a table exists with the given schema, otherwise create it
    # 2. Add a row to the table for a given iid (instance_id, store_path, timestamp)
    # 3. Get results from the table (i.e. all or just the latest) rows for a given iid

