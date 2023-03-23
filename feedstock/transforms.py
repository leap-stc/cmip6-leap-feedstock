"""Modified transforms from Pangeo Forge"""

from dataclasses import dataclass, field
from typing import List, Dict
from pangeo_forge_recipes.patterns import Dimension
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import DetermineSchema, XarraySchema, IndexItems, PrepareZarrTarget, StoreDatasetFragments
from pangeo_forge_recipes.aggregation import schema_to_template_ds

import apache_beam as beam
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, StoreToZarr

def dynamic_target_chunks_from_schema(
    schema: XarraySchema, 
    target_chunk_nbytes: int = None,
    chunk_dim: str = None
) -> dict[str, int]:
    """Dynamically determine target_chunks from schema based on desired chunk size"""
    # convert schema to dataset
    ds = schema_to_template_ds(schema)
    
    # create full chunk dictionary for all other dimensions
    target_chunks = {k: len(ds[k]) for k in ds.dims if k != chunk_dim}
    
    # get size of dataset
    nbytes = ds.nbytes
    
    # get size of single chunk along `chunk_dim`
    nbytes_single = nbytes/len(ds[chunk_dim])
    
    if nbytes_single > target_chunk_nbytes:
        # if a single element chunk along `chunk_dim` is larger than the target, we have no other choice than exceeding that limit
        # Chunking along another dimension would work, but makes this way more complicated.
        # TODO: Should raise a warnign
        chunk_size = 1
        
    else:
        # determine chunksize (staying under the given limit)
        chunk_size = target_chunk_nbytes//nbytes_single
        
    target_chunks[chunk_dim] = chunk_size
    return target_chunks

@dataclass
class StoreToZarrLegacyDynamic(beam.PTransform):
    """Store a PCollection of Xarray datasets to Zarr.
    :param combine_dims: The dimensions to combine
    :param target_root: Location the Zarr store will be created inside.
    :param store_name: Name for the Zarr store. It will be created with this name
                       under `target_root`.
    :param target_chunks: Dictionary mapping dimension names to chunks sizes.
        If a dimension is a not named, the chunks will be inferred from the data.
    """

    # TODO: make it so we don't have to explictly specify combine_dims
    # Could be inferred from the pattern instead
    combine_dims: List[Dimension]
    target_root: str | FSSpecTarget
    store_name: str
    target_chunk_nbytes : int
    chunk_dim : str
    target_chunks: Dict[str, int] = field(default_factory=dict)

    def expand(self, datasets: beam.PCollection) -> beam.PCollection:
        schema = datasets | DetermineSchema(combine_dims=self.combine_dims)
        self.target_chunks = schema | beam.Map(dynamic_target_chunks_from_schema, 
                                               target_chunk_nbytes=self.target_chunk_nbytes, 
                                               chunk_dim=self.chunk_dim)
        indexed_datasets = datasets | IndexItems(schema=schema)
        if isinstance(self.target_root, str):
            target_root = FSSpecTarget.from_url(self.target_root)
        else:
            target_root = self.target_root
        full_target = target_root / self.store_name
        target_store = schema | PrepareZarrTarget(
            target=full_target, target_chunks=beam.pvalue.AsSingleton(self.target_chunks)
        )
        return indexed_datasets | StoreDatasetFragments(target_store=target_store)