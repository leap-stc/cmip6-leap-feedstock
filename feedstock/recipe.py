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

iid = get_iid() # this gets injected by pangeo-forge-runner
jobname = get_jobname()

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

# just to be sure that only the actual variable_id is used as a dataset variable
class KeepOnlyVariableId(beam.PTransform):
    """
    Set all data_variables except for `variable_id` attrs to coord
    """
    
    @staticmethod
    def _keep_only_variable_id(item: Indexed[T]) -> Indexed[T]:
        """
        Many netcdfs contain variables other than the one specified in the `variable_id` facet. 
        Set them all to coords
        """
        index, ds = item
        new_coords_vars = [var for var in ds.data_vars if var != ds.attrs['variable_id']]
        ds = ds.set_coords(new_coords_vars)
        return index, ds
    
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._keep_only_variable_id)

# create recipe dictionary
target_chunk_nbytes = int(100e6)
input_urls = urls_from_gcs(iid) # The iid input here gets ingected from pangeo-forge-runner (https://github.com/pangeo-forge/pangeo-forge-runner/pull/67)
print(f'Creating recipe from {input_urls}')
pattern = pattern_from_file_sequence(input_urls, concat_dim='time')
transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(xarray_open_kwargs={"use_cftime":True, "decode_coords": "all"}) # do not specify file type to accomdate both ncdf3 and ncdf4
    # | KeepOnlyVariableId() # disable to see if necessary
    | StoreToZarr(
        store_name=f"{jobname}.zarr", # use jobname for now to have a unique store? 
        combine_dims=pattern.combine_dim_keys,
        target_chunk_nbytes=target_chunk_nbytes,
        chunk_dim='time'
    )
)
