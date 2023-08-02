from google.cloud import bigquery
from typing import List, Union, Dict, Optional
from dataclasses import dataclass
import datetime

@dataclass
class BigQueryIIDResult:
    """Class to handle the results pertaining to a single IID. 
    Assumes and checks that there are no duplicate rows for the IID"""
    results: bigquery.table.RowIterator
    iid: str
    
    def __post_init__(self):
        res_iterator = self.results.result() #waits for the query to be executed
        # not sure if I should wait here or in the interface class?
            
        assert res_iterator.total_rows <= 1
        
        self.exists:bool = res_iterator.total_rows > 0
        self.has_urls:bool = self.exists
        if self.exists:
            [row] = [r for r in res_iterator]
            self.row = row
            self.has_urls = len(self.row['urls'])>0
        
    

@dataclass
class BigQueryInterface:
    """Class to read/write information to the CMIP6 BQ database"""
    client: bigquery.client.Client
    table_id: str
    
    def _get_query_job(self, iid:str, limit=1) -> bigquery.job.query.QueryJob:
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
        LIMIT {limit}
        """
        return self.client.query(query)
    
    def get_iid_results(self, iid: str) -> BigQueryIIDResult:
        results = self._get_query_job(iid)
        return BigQueryIIDResult(results, iid)
    
    
    def add_row(self, iid_obj:BigQueryIIDResult, fields:Dict[str, Union[str, list]], force:bool=False):
        """Add new row to BigQuery Table"""
        
        if not force and iid_obj.exists:
            # setting force to false will not touch entries not given in `values`
            row = iid_obj.row
            default_fields = {k: row[k] for k in row.keys()}
            fields = default_fields | fields
            
        # always replace the timestamp!
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        fields['timestamp'] = now
        # always carry the instance_id
        fields['instance_id'] = iid_obj.iid

        rows_to_insert = [fields]
        errors = self.client.insert_rows_json(self.table_id, rows_to_insert)  # Make an API request.
        return errors
    
    
# def was_written():
#     pass

# def passed_tests():
#     pass