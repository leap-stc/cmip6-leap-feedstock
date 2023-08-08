### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import apache_beam as beam
from dataclasses import dataclass
import datetime
from typing import List
import json
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec, OpenWithXarray, StoreToZarr, Indexed, T
)

@dataclass
class Preprocessor(beam.PTransform):
    """
    Preprocessor for xarray datasets.
    Set all data_variables except for `variable_id` attrs to coord
    Add additional information 

    :param urls: List of urls to the files to be opened
    """
    urls: List[str] #??? @cisaacstern Is there a way to get this info from the pipeline?

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

    def _add_bake_info(self,item: Indexed[T]) -> Indexed[T]:
        """
        Add the exact urls and the time stamp to the dataset attributes
    
        """
        index, ds = item
        ds.attrs['pangeo_forge_bake_urls'] = self.urls
        ds.attrs['pangeo_forge_bake_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return index, ds
    
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return ( pcoll 
            | "Fix coordinates" >> beam.Map(self._keep_only_variable_id) 
            | "Write bake info to datasets " >> beam.Map(self._add_bake_info)
        )


# NOTE: This is a simplified setup, mainly to test the changes to StoreToZarr
# here (https://github.com/pangeo-forge/pangeo-forge-recipes/pull/546). I 
# will start a new PR to try to refactor this as a dict-object, after 
# checking in with Charles (https://github.com/leap-stc/cmip6-leap-feedstock/pull/4#issuecomment-1666929555)

with open('feedstock/first_batch.json') as json_file:
    url_dict = json.load(json_file)

target_chunks_aspect_ratio = {'time': 1}

recipes = {}
for iid, urls in url_dict.items():
    pattern = pattern_from_file_sequence(
        urls,
        concat_dim='time'
        )
    recipes[iid] = (
        beam.Create(pattern.items())
        | OpenURLWithFSSpec()
        | OpenWithXarray(xarray_open_kwargs={"use_cftime":True}) # do not specify file type to accomodate both ncdf3 and ncdf4
        | Preprocessor(urls=urls)
        | StoreToZarr(
            store_name=f"{iid}.zarr",
            combine_dims=pattern.combine_dim_keys,
            target_chunk_size='200MB',
            target_chunks_aspect_ratio = target_chunks_aspect_ratio,
            size_tolerance=0.5
            )
        )
