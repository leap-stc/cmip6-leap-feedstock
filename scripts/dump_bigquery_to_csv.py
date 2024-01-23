from leap_data_management_utils import CMIPBQInterface
from leap_data_management_utils.cmip_catalog import bq_df_to_intake_esm
import pandas as pd
import os
import gcsfs
from datetime import date

table_id = 'leap-pangeo.testcmip6.cmip6_consolidated_manual_testing'
target_prefix = "gs://cmip6/cmip6-pgf-ingestion-test/catalog/"

bq = CMIPBQInterface(table_id=table_id)

df_all = bq.get_latest()

df_retracted = df_all[df_all['retracted'] == True]
df_all_wo_retracted = df_all[df_all['retracted'] == False]

df_qc_failed = df_all_wo_retracted[df_all_wo_retracted['tests_passed'] == False]
df_qc = df_all_wo_retracted[df_all_wo_retracted['tests_passed'] == True]

fs = gcsfs.GCSFileSystem()

for filename, bq_df in [
    ("qc", df_qc),
    ("noqc", df_qc_failed),
    ("retracted", df_retracted),
]:
    path = os.path.join(
        target_prefix, 
        f"pangeo_esgf_zarr_{filename}.csv"
    )
    path_backup = os.path.join(
        target_prefix, 
        'backups',
        f"pangeo_esgf_zarr_{filename}_backup_{date.today()}.csv"
    )
    
    if len(bq_df)> 0:
        intake_esm_df = bq_df_to_intake_esm(bq_df)
        intake_esm_df.to_csv(filename)
        if fs.exists(path):
            fs.cp(path, path_backup)
        fs.put_file(filename, path)
