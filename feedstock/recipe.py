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
    # return iid
    return 'CMIP6.CMIP.MRI.MRI-ESM2-0.historical.r3i1p1f1.day.pr.gn.v20190603'

iid = get_iid() # The iid input here gets ingected from pangeo-forge-runner (https://github.com/pangeo-forge/pangeo-forge-runner/pull/67)

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

# new version that uses the BQ database
def urls_from_bq(iid: str) -> List[str]:
    # """Get URLS from bigquery"""
    # from bigquery import BigQueryInterface # this naming is dicey...
    # from google.cloud import bigquery

    # table_id = 'leap-pangeo.testcmip6.stores_v2'
    # client = bigquery.Client()
    # bq_interface = BigQueryInterface(client, table_id)
    # iid_obj = bq_interface.get_iid_results(iid)
    # print(f"{iid_obj.exists=}")
    # print(iid_obj.results)

    # return list(iid_obj.results)[0][2] #FIXME: This is pretty ugly...just a quick test for now.
    
    # !!!Bypass for now and hardcode the urls
    return ['http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/MRI/MRI-ESM2-0/historical/r3i1p1f1/day/pr/gn/v20190603/pr_day_MRI-ESM2-0_historical_r3i1p1f1_gn_18500101-18991231.nc',
 'http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/MRI/MRI-ESM2-0/historical/r3i1p1f1/day/pr/gn/v20190603/pr_day_MRI-ESM2-0_historical_r3i1p1f1_gn_19000101-19491231.nc',
 'http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/MRI/MRI-ESM2-0/historical/r3i1p1f1/day/pr/gn/v20190603/pr_day_MRI-ESM2-0_historical_r3i1p1f1_gn_19500101-19991231.nc',
 'http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/MRI/MRI-ESM2-0/historical/r3i1p1f1/day/pr/gn/v20190603/pr_day_MRI-ESM2-0_historical_r3i1p1f1_gn_20000101-20141231.nc']


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

# # debug the recipe locally
# from pangeo_forge_recipes.aggregation import XarraySchema
# from pangeo_forge_recipes.dynamic_target_chunks import dynamic_target_chunks_from_schema
# from pangeo_forge_recipes.patterns import Dimension
# from pangeo_forge_recipes.writers import ZarrWriterMixin
# from pangeo_forge_recipes.transforms import DetermineSchema, IndexItems, Rechunk, PrepareZarrTarget, StoreDatasetFragments
# from dataclasses import dataclass, field

# from typing import Dict, Optional, Union, cast

# @dataclass
# class StoreToZarr(beam.PTransform, ZarrWriterMixin):
#     """Store a PCollection of Xarray datasets to Zarr.

#     :param combine_dims: The dimensions to combine
#     :param target_chunks: Dictionary mapping dimension names to chunks sizes.
#         If a dimension is a not named, the chunks will be inferred from the data.
#     :param target_chunk_aspect_ratio: Dictionary mapping dimension names to desired
#         aspect ratio of total number of chunks along each dimension.
#     :param target_chunk_size: Desired single chunks size. Can be provided as
#         integer (bytes) or as a str like '100MB' etc.
#     :param size_tolerance : float, optional
#         Chunksize tolerance. Resulting chunk size will be within
#         [target_chunk_size*(1-size_tolerance),
#         target_chunk_size*(1+size_tolerance)] , by default 0.2
#     """

#     # TODO: make it so we don't have to explicitly specify combine_dims
#     # Could be inferred from the pattern instead
#     combine_dims: List[Dimension]
#     target_chunks: Optional[Dict[str, int]] = field(default=None)
#     target_chunks_aspect_ratio: Optional[Dict[str, int]] = field(default=None)
#     target_chunk_size: Optional[Union[str, int]] = field(
#         default=None
#     )  # ? Should we provide a default?
#     size_tolerance: float = 0.2

#     def __post_init__(self):
#         # if none of the chunking parameters is specified, set the default behavior
#         # of target_chunks={}
#         if all(
#             a is None
#             for a in (self.target_chunks, self.target_chunk_size, self.target_chunks_aspect_ratio)
#         ):
#             self.target_chunks = {}

#         # check that not both static and dynamic chunking are specified
#         if self.target_chunks is not None and (
#             self.target_chunk_size is not None or self.target_chunks_aspect_ratio is not None
#         ):
#             raise ValueError(
#                 (
#                     "Cannot specify both target_chunks and "
#                     "target_chunk_size or target_chunks_aspect_ratio."
#                 )
#             )

#         # if dynamic chunking is specified, make sure both target_chunk_size
#         # and target_chunks_aspect_ratio are specified
#         if self.target_chunks is None and not (
#             self.target_chunk_size is not None and self.target_chunks_aspect_ratio is not None
#         ):
#             raise ValueError(
#                 (
#                     "Must specify both target_chunk_size and "
#                     "target_chunks_aspect_ratio to enable dynamic chunking."
#                 )
#             )

#     def determine_target_chunks(self, schema: XarraySchema) -> Dict[str, int]:
#         # if dynamic chunking is chosen, set the objects target_chunks here
#         # We need to explicitly cast the types here because we know
#         # that for the combinations chosen here (dynamic vs static chunking)
#         # the type of the input is never None (our __post_init__ method takes
#         # care of raising errors if this is the case).
#         if self.target_chunks_aspect_ratio is not None:
#             target_chunks = dynamic_target_chunks_from_schema(
#                 schema,
#                 target_chunks = {'time':10},
#                 # target_chunk_size=cast(Union[int, str], self.target_chunk_size),
#                 # target_chunks_aspect_ratio=cast(Dict[str, int], self.target_chunks_aspect_ratio),
#                 # size_tolerance=self.size_tolerance,
#             )
#         else:
#             target_chunks = cast(Dict[str, int], self.target_chunks)
#         return target_chunks

#     def expand(self, datasets: beam.PCollection) -> beam.PCollection:
#         schema = datasets | DetermineSchema(combine_dims=self.combine_dims)

#         target_chunks = beam.pvalue.AsSingleton(
#             schema | beam.Map(
#                 self.determine_target_chunks
#             )  # Would beam.pvalue.AsSingleton(self.determine_target_chunks(schema)) work?
#         )
#         indexed_datasets = datasets | IndexItems(schema=schema)
#         rechunked_datasets = indexed_datasets | Rechunk(target_chunks=target_chunks, schema=schema)
#         target_store = schema | PrepareZarrTarget(
#             target=self.get_full_target(), target_chunks=target_chunks
#         )
#         return rechunked_datasets | StoreDatasetFragments(target_store=target_store)



# create recipe dictionary
input_urls = urls_from_bq(iid)
print(f'Creating recipe from {input_urls}')
pattern = pattern_from_file_sequence(input_urls, concat_dim='time')
TEST_CMIP = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(xarray_open_kwargs={"use_cftime":True}) # do not specify file type to accomdate both ncdf3 and ncdf4
    | KeepOnlyVariableId()
    | StoreToZarr(
        store_name=f"{iid}.zarr",
        combine_dims=pattern.combine_dim_keys,
        # for now try with static chunking
        # target_chunks={'time':1000},
        target_chunk_size='200MB',
        target_chunks_aspect_ratio = {'lon': 1, 'lat': 1, 'time': 10, 'bnds':2}, # TODO: this will fail for many of the datasets with different naming. 
        # Need to improve this upstream (but how do I generalize this?)
    )
)