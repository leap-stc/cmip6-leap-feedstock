from leap_data_management_utils import CMIPBQInterface, IIDEntry
import pandas as pd
import numpy as np
import warnings
from google.cloud import bigquery
from pangeo_forge_esgf.utils import CMIP6_naming_schema

# def _maybe_prepend_dummy_dcpp(s: str):
#     if not "-" in s:
#         return "dummy-" + s
#     else:
#         return s


# def convert_bq_to_cmip6_df(table_id: str) -> pd.DataFrame:
#     """Convert a bigquery table to a pandas dataframe
#     conforming to the old pangeo cmip6 catalog format
#     :param table_id: BigQuery table ID
#     :returns: pandas dataframe
#     """
#     print('Start conversion')
#     bq = CMIPBQInterface(table_id)
#     latest_rows = bq.get_latest()
#     if len(latest_rows) == 0:
#         warnings.warn(f"No rows in {table_id=}")
#     else:
#         print(f"Found {len(latest_rows)} rows in {table_id=}")
#         iid_facets = CMIP6_naming_schema.split(".")
#         # some legit pandas magic here: https://stackoverflow.com/a/39358924
#         df = latest_rows["instance_id"].str.split(".", expand=True)
#         df = df.rename(columns={i:f for i,f in enumerate(iid_facets)})
#         df["zstore"] = "gs://" + latest_rows["store"]

#         # expand member_id into dcpp_init_year and variant_label
#         # (relabelling variant_label to member_id. I think this is wrong,
#         # but keeping it for backwards compatibility)
#         df[["dcpp_init_year", "member_id"]] = (df["variant_label"].apply(lambda s: _maybe_prepend_dummy_dcpp(s)).str.split("-", expand=True))
#         df.replace('dummy', np.nan, inplace=True)

#         # order the columns in the order of the old csv (not sure if this is necessary)
#         df = df[
#             [
#                 "activity_id",
#                 "institution_id",
#                 "source_id",
#                 "experiment_id",
#                 "member_id",
#                 "table_id",
#                 "variable_id",
#                 "grid_label",
#                 "zstore",
#                 "dcpp_init_year",
#                 "version",
#             ]
#         ]
#         return df

# def _maybe_join(iterable):
#     assert len(iterable) == 2 
#     dcpp_init_year = iterable[0]
#     member_id = iterable[1]
#     if not pd.isnull(dcpp_init_year):
#         return f"{dcpp_init_year}-{member_id}"
#     else:
#         return member_id

# def convert_cmip6_df_to_iid_df(df: pd.DataFrame) -> pd.DataFrame:
#     # now remove the ones already in the pangeo catalog

#     df['variant_label'] = df[['dcpp_init_year', 'member_id']].agg(_maybe_join, axis=1)
#     df['version'] = 'v'+df['version'].astype(str)
#     df['instance_id'] = df[['activity_id', 'institution_id', 'source_id', 'experiment_id',
#         'variant_label', 'table_id', 'variable_id', 'grid_label', 'version']].astype(str).agg('.'.join, axis=1).tolist()
#     df['instance_id'] = 'CMIP6.'+df['instance_id']
#     df['store'] = df['zstore']
#     # add current time as bigquery timestamp
#     df['timestamp'] = pd.Timestamp.now(tz='UTC')
#     df = df[['instance_id','store', 'timestamp']]
#     return df
    
def upload_cmip6_df_to_bq(df: pd.DataFrame, table_id:str, ):
    """Upload a pandas dataframe to a bigquery table
    :param df: pandas dataframe
    :param table_id: BigQuery table ID
    **Currently this always overwrites the tabel!**
    """
    # should this live in bigquery_interface.py?
    # TODO: Figure out how to *NOT* overwrite the table and add an `overwrite` kwarg

    # From https://stackoverflow.com/a/57226898
    client = bigquery.Client()

    job_config = bigquery.job.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()



