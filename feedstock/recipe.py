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

iid_a = 'CMIP6.CMIP.MRI.MRI-ESM2-0.historical.r3i1p1f1.day.pr.gn.v20190603'
pattern_a = pattern_from_file_sequence(
    url_dict[iid_a],
    concat_dim='time'
    )
template_a = (
        beam.Create(pattern_a.items())
        | OpenURLWithFSSpec()
        | OpenWithXarray(xarray_open_kwargs={"use_cftime":True}) # do not specify file type to accomdate both ncdf3 and ncdf4
        | KeepOnlyVariableId()
        | StoreToZarr(
            store_name=f"{iid_a}.zarr",
            combine_dims=pattern_a.combine_dim_keys,
            target_chunk_size='200MB',
            target_chunks_aspect_ratio = target_chunks_aspect_ratio,
            size_tolerance=0.5,
        )
    )

iid_b = 'CMIP6.CMIP.NOAA-GFDL.GFDL-CM4.historical.r1i1p1f1.Amon.tas.gr1.v20180701'
pattern_b = pattern_from_file_sequence(
    url_dict[iid_b],
    concat_dim='time'
    )

template_b = (
        beam.Create(pattern_b.items())
        | OpenURLWithFSSpec()
        | OpenWithXarray(xarray_open_kwargs={"use_cftime":True}) # do not specify file type to accomdate both ncdf3 and ncdf4
        | KeepOnlyVariableId()
        | StoreToZarr(
            store_name=f"{iid_b}.zarr",
            combine_dims=pattern_b.combine_dim_keys,
            target_chunk_size='200MB',
            target_chunks_aspect_ratio = target_chunks_aspect_ratio,
        )
    )

# Old code (reling on injections)
# def get_iid(iid: str=None) -> str:
#     """pangeo-forge-runner injection func"""
#     # return iid
#     # return 'CMIP6.CMIP.MRI.MRI-ESM2-0.historical.r3i1p1f1.day.pr.gn.v20190603'
#     return 

# iid = get_iid() # The iid input here gets ingected from pangeo-forge-runner (https://github.com/pangeo-forge/pangeo-forge-runner/pull/67)

# def urls_from_gcs(iid: str) -> List[str]:
#     """Get urls from GCS bucket"""
#     import gcsfs
#     import json
#     print(f"Fetching urls from GCS for {iid}")
#     url_bucket = 'leap-persistent/jbusecke/cmip6urls'
#     fs = gcsfs.GCSFileSystem(project='leap-pangeo')
#     with fs.open(f"gs://{url_bucket}/{iid}.json", 'r') as f:
#         urls = json.load(f)['urls']
#     return urls

# # new version that uses the BQ database
# def urls_from_bq(iid: str) -> List[str]:
#     # """Get URLS from bigquery"""
#     # from bigquery import BigQueryInterface # this naming is dicey...
#     # from google.cloud import bigquery

#     # table_id = 'leap-pangeo.testcmip6.stores_v2'
#     # client = bigquery.Client()
#     # bq_interface = BigQueryInterface(client, table_id)
#     # iid_obj = bq_interface.get_iid_results(iid)
#     # print(f"{iid_obj.exists=}")
#     # print(iid_obj.results)

#     # return list(iid_obj.results)[0][2] #FIXME: This is pretty ugly...just a quick test for now.
#     return url_dict[iid]
