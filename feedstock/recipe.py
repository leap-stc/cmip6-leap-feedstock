### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import apache_beam as beam
from typing import List
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec, OpenWithXarray, StoreToZarr, Indexed, T
)

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


# NOTE: This is a simplified setup, mainly to test the changes to StoreToZarr
# here (https://github.com/pangeo-forge/pangeo-forge-recipes/pull/546). I 
# will start a new PR to try to refactor this as a dict-object, after 
# checking in with Charles (https://github.com/leap-stc/cmip6-leap-feedstock/pull/4#issuecomment-1666929555)

iid_list = [
    'CMIP6.CMIP.MRI.MRI-ESM2-0.historical.r3i1p1f1.day.pr.gn.v20190603',
    'CMIP6.CMIP.NOAA-GFDL.GFDL-CM4.historical.r1i1p1f1.Amon.tas.gr1.v20180701'
    ]

# for now use a simple hardcoded dict (TODO: replace this with either a call to bigquery or some ESGF API call)
url_dict = {
        'CMIP6.CMIP.MRI.MRI-ESM2-0.historical.r3i1p1f1.day.pr.gn.v20190603':[
            'http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/MRI/MRI-ESM2-0/historical/r3i1p1f1/day/pr/gn/v20190603/pr_day_MRI-ESM2-0_historical_r3i1p1f1_gn_18500101-18991231.nc',
            'http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/MRI/MRI-ESM2-0/historical/r3i1p1f1/day/pr/gn/v20190603/pr_day_MRI-ESM2-0_historical_r3i1p1f1_gn_19000101-19491231.nc',
            'http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/MRI/MRI-ESM2-0/historical/r3i1p1f1/day/pr/gn/v20190603/pr_day_MRI-ESM2-0_historical_r3i1p1f1_gn_19500101-19991231.nc',
            'http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/MRI/MRI-ESM2-0/historical/r3i1p1f1/day/pr/gn/v20190603/pr_day_MRI-ESM2-0_historical_r3i1p1f1_gn_20000101-20141231.nc'
        ],
        'CMIP6.CMIP.NOAA-GFDL.GFDL-CM4.historical.r1i1p1f1.Amon.tas.gr1.v20180701': [
            'http://esgf-data04.diasjp.net/thredds/fileServer/esg_dataroot/CMIP6/CMIP/NOAA-GFDL/GFDL-CM4/historical/r1i1p1f1/Amon/tas/gr1/v20180701/tas_Amon_GFDL-CM4_historical_r1i1p1f1_gr1_185001-194912.nc',
            'http://esgf-data04.diasjp.net/thredds/fileServer/esg_dataroot/CMIP6/CMIP/NOAA-GFDL/GFDL-CM4/historical/r1i1p1f1/Amon/tas/gr1/v20180701/tas_Amon_GFDL-CM4_historical_r1i1p1f1_gr1_195001-201412.nc'
        ],
    }

target_chunks_aspect_ratio = {'time': 1}

recipes = {}
for iid in iid_list:
    pattern = pattern_from_file_sequence(
        url_dict[iid],
        concat_dim='time'
        )
    recipes[iid] = (
        beam.Create(pattern.items())
        | OpenURLWithFSSpec()
        | OpenWithXarray(xarray_open_kwargs={"use_cftime":True}) # do not specify file type to accomdate both ncdf3 and ncdf4
        | KeepOnlyVariableId()
        | StoreToZarr(
            store_name=f"{iid}.zarr",
            combine_dims=pattern.combine_dim_keys,
            target_chunk_size='150MB',
            target_chunks_aspect_ratio = target_chunks_aspect_ratio,
            size_tolerance=0.3
            )
        )
