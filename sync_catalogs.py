
from catalog_utils import convert_bq_to_cmip6_df, convert_cmip6_df_to_iid_df, upload_cmip6_df_to_bq
import pandas as pd


# I need a query to get the latest row for each instance_id
#TODO: these need to be centrally defined
table_id_legacy = "leap-pangeo.testcmip6.cmip6_legacy"
table_id = "leap-pangeo.testcmip6.cmip6_feedstock_test2"
table_id_nonqc = "leap-pangeo.testcmip6.cmip6_feedstock_test2_nonqc"

# write out csv from bq
df_qc = convert_bq_to_cmip6_df(table_id)
df_nonqc = convert_bq_to_cmip6_df(table_id_nonqc)

# write to csv
if df_qc is not None:
    df_qc.to_csv("leap-pangeo-cmip6-test.csv", index=False)
if df_nonqc is not None:
    df_nonqc.to_csv("leap-pangeo-cmip6-noqc-test.csv", index=False)

# write bq from legacy csv (??? Should we do this in a separate script?)
df_legacy = pd.read_csv('https://storage.googleapis.com/cmip6/pangeo-cmip6.csv')

upload_cmip6_df_to_bq(convert_cmip6_df_to_iid_df(df_legacy), table_id_legacy)
