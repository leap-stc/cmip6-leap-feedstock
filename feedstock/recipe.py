### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import apache_beam as beam
from typing import List
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec, OpenWithXarray, StoreToZarr, Indexed, T
)

def get_iid(iid: str=None) -> str:
    """pangeo-forge-runner injection func"""
    return iid

def get_jobname(jobname: str=None) -> str:
    """pangeo-forge-runner injection func"""
    return jobname

iid = get_iid() # The iid input here gets ingected from pangeo-forge-runner (https://github.com/pangeo-forge/pangeo-forge-runner/pull/67)
jobname = get_jobname() # same here with the jobname

def urls_from_gcs(iid: str) -> List[str]:
    """Get urls from GCS bucket"""
    import gcsfs
    import json
    print(f"Fetching urls from GCS for {iid}")
    url_bucket = 'leap-persistent/jbusecke/cmip6urls'
    fs = gcsfs.GCSFileSystem(project='leap-pangeo')
    with fs.open(f"gs://{url_bucket}/{iid}.json", 'r') as f:
        urls = json.load(f)['urls']
    return urls

# create recipe dictionary
target_chunk_nbytes = int(100e6)
input_urls = urls_from_gcs(iid)
print(f'Creating recipe from {input_urls}')
pattern = pattern_from_file_sequence(input_urls, concat_dim='time')
transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(xarray_open_kwargs={"use_cftime":True}) # do not specify file type to accomdate both ncdf3 and ncdf4
    # | KeepOnlyVariableId() # disable to see if necessary
    | StoreToZarr(
        store_name=f"{iid}.zarr", 
        combine_dims=pattern.combine_dim_keys,
        target_chunk_nbytes=target_chunk_nbytes,
        chunk_dim='time'
    )
)
