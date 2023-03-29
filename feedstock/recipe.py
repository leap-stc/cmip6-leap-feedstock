### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""
import json
import apache_beam as beam
from dataclasses import dataclass, field
from typing import List, Dict, Union
from pangeo_forge_recipes.patterns import Dimension, pattern_from_file_sequence
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import DetermineSchema, XarraySchema, IndexItems, PrepareZarrTarget, StoreDatasetFragments
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray

#### hardcode the json here (this is stupid)
recipe_input_dict = {'CMIP6.CMIP.MOHC.UKESM1-0-LL.historical.r19i1p1f2.Omon.zmeso.gn.v20200430': {'urls': ['https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/MOHC/UKESM1-0-LL/historical/r19i1p1f2/Omon/zmeso/gn/v20200430/zmeso_Omon_UKESM1-0-LL_historical_r19i1p1f2_gn_185001-189912.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/MOHC/UKESM1-0-LL/historical/r19i1p1f2/Omon/zmeso/gn/v20200430/zmeso_Omon_UKESM1-0-LL_historical_r19i1p1f2_gn_190001-194912.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/MOHC/UKESM1-0-LL/historical/r19i1p1f2/Omon/zmeso/gn/v20200430/zmeso_Omon_UKESM1-0-LL_historical_r19i1p1f2_gn_195001-199912.nc'],
  'instance_id': 'CMIP6.CMIP.MOHC.UKESM1-0-LL.historical.r19i1p1f2.Omon.zmeso.gn.v20200430',
  'data_node': 'esgf-data1.llnl.gov'},
 'CMIP6.CMIP.CCCma.CanESM5-CanOE.historical.r2i1p2f1.Omon.zmeso.gn.v20190429': {'urls': ['http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_185001-186012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_186101-187012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_187101-188012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_188101-189012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_189101-190012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_190101-191012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_191101-192012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_192101-193012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_193101-194012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_194101-195012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_195101-196012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_196101-197012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_197101-198012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_198101-199012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_199101-200012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_200101-201012.nc',
   'http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgA_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5-CanOE/historical/r2i1p2f1/Omon/zmeso/gn/v20190429/zmeso_Omon_CanESM5-CanOE_historical_r2i1p2f1_gn_201101-201412.nc'],
  'instance_id': 'CMIP6.CMIP.CCCma.CanESM5-CanOE.historical.r2i1p2f1.Omon.zmeso.gn.v20190429',
  'data_node': 'crd-esgf-drc.ec.gc.ca'},
 'CMIP6.CMIP.MOHC.UKESM1-0-LL.historical.r4i1p1f2.Omon.zmeso.gn.v20200204': {'urls': ['https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/CMIP/MOHC/UKESM1-0-LL/historical/r4i1p1f2/Omon/zmeso/gn/v20200204/zmeso_Omon_UKESM1-0-LL_historical_r4i1p1f2_gn_185001-189912.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/CMIP/MOHC/UKESM1-0-LL/historical/r4i1p1f2/Omon/zmeso/gn/v20200204/zmeso_Omon_UKESM1-0-LL_historical_r4i1p1f2_gn_190001-194912.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/CMIP/MOHC/UKESM1-0-LL/historical/r4i1p1f2/Omon/zmeso/gn/v20200204/zmeso_Omon_UKESM1-0-LL_historical_r4i1p1f2_gn_195001-199912.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/CMIP/MOHC/UKESM1-0-LL/historical/r4i1p1f2/Omon/zmeso/gn/v20200204/zmeso_Omon_UKESM1-0-LL_historical_r4i1p1f2_gn_200001-201412.nc'],
  'instance_id': 'CMIP6.CMIP.MOHC.UKESM1-0-LL.historical.r4i1p1f2.Omon.zmeso.gn.v20200204',
  'data_node': 'esgf.ceda.ac.uk'},
 'CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.Omon.zmeso.gn.v20180803': {'urls': ['http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r1i1p1f1/Omon/zmeso/gn/v20180803/zmeso_Omon_IPSL-CM6A-LR_historical_r1i1p1f1_gn_185001-194912.nc',
   'http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r1i1p1f1/Omon/zmeso/gn/v20180803/zmeso_Omon_IPSL-CM6A-LR_historical_r1i1p1f1_gn_195001-201412.nc'],
  'instance_id': 'CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.Omon.zmeso.gn.v20180803',
  'data_node': 'aims3.llnl.gov'},
 'CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r14i1p1f1.Omon.zmeso.gn.v20180803': {'urls': ['http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r14i1p1f1/Omon/zmeso/gn/v20180803/zmeso_Omon_IPSL-CM6A-LR_historical_r14i1p1f1_gn_185001-194912.nc',
   'http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r14i1p1f1/Omon/zmeso/gn/v20180803/zmeso_Omon_IPSL-CM6A-LR_historical_r14i1p1f1_gn_195001-201412.nc'],
  'instance_id': 'CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r14i1p1f1.Omon.zmeso.gn.v20180803',
  'data_node': 'aims3.llnl.gov'},
 'CMIP6.CMIP.IPSL.IPSL-CM5A2-INCA.historical.r1i1p1f1.Omon.zmeso.gn.v20200729': {'urls': ['https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/IPSL/IPSL-CM5A2-INCA/historical/r1i1p1f1/Omon/zmeso/gn/v20200729/zmeso_Omon_IPSL-CM5A2-INCA_historical_r1i1p1f1_gn_185001-201412.nc'],
  'instance_id': 'CMIP6.CMIP.IPSL.IPSL-CM5A2-INCA.historical.r1i1p1f1.Omon.zmeso.gn.v20200729',
  'data_node': 'esgf-data1.llnl.gov'},
 'CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r27i1p1f1.Omon.zmeso.gn.v20191204': {'urls': ['http://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r27i1p1f1/Omon/zmeso/gn/v20191204/zmeso_Omon_IPSL-CM6A-LR_historical_r27i1p1f1_gn_185001-194912.nc',
   'http://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r27i1p1f1/Omon/zmeso/gn/v20191204/zmeso_Omon_IPSL-CM6A-LR_historical_r27i1p1f1_gn_195001-201412.nc'],
  'instance_id': 'CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r27i1p1f1.Omon.zmeso.gn.v20191204',
  'data_node': 'esgf-data1.llnl.gov'},
 'CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r29i1p1f1.Omon.zmeso.gn.v20191204': {'urls': ['http://vesg.ipsl.upmc.fr/thredds/fileServer/cmip6/CMIP/IPSL/IPSL-CM6A-LR/historical/r29i1p1f1/Omon/zmeso/gn/v20191204/zmeso_Omon_IPSL-CM6A-LR_historical_r29i1p1f1_gn_185001-194912.nc',
   'http://vesg.ipsl.upmc.fr/thredds/fileServer/cmip6/CMIP/IPSL/IPSL-CM6A-LR/historical/r29i1p1f1/Omon/zmeso/gn/v20191204/zmeso_Omon_IPSL-CM6A-LR_historical_r29i1p1f1_gn_195001-201412.nc'],
  'instance_id': 'CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r29i1p1f1.Omon.zmeso.gn.v20191204',
  'data_node': 'vesg.ipsl.upmc.fr'}}
######

# # load recipe input dictionary from json file

# with open('recipe_input_dict.json', 'r') as f:
#     recipe_input_dict = json.load(f)

def dynamic_target_chunks_from_schema(
    schema: XarraySchema, 
    target_chunk_nbytes: int = None,
    chunk_dim: str = None
) -> dict[str, int]:
    """Dynamically determine target_chunks from schema based on desired chunk size"""
    # convert schema to dataset
    from pangeo_forge_recipes.aggregation import schema_to_template_ds # weird but apparently necessary for dataflow
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
    return {k:int(v) for k,v in target_chunks.items()} # make sure the values are integers, maybe this fixes the dataflow error

@dataclass
class StoreToZarr(beam.PTransform):
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
    # target_root: Union[str, FSSpecTarget] # temp renamed (bug)
    target: Union[str, FSSpecTarget]
    # store_name: str
    target_chunk_nbytes : int
    chunk_dim : str
    target_chunks: Dict[str, int] = field(default_factory=dict)

    def expand(self, datasets: beam.PCollection) -> beam.PCollection:
        schema = datasets | DetermineSchema(combine_dims=self.combine_dims)
        self.target_chunks = schema | beam.Map(dynamic_target_chunks_from_schema, 
                                               target_chunk_nbytes=self.target_chunk_nbytes, 
                                               chunk_dim=self.chunk_dim)
        indexed_datasets = datasets | IndexItems(schema=schema)
        if isinstance(self.target, str):
            target = FSSpecTarget.from_url(self.target)
        else:
            target = self.target
        # full_target = target / self.store_name
        target_store = schema | PrepareZarrTarget(
            target=target, target_chunks=beam.pvalue.AsSingleton(self.target_chunks)
        )
        return indexed_datasets | StoreDatasetFragments(target_store=target_store)




# create recipe dictionary
target_chunk_nbytes = int(100e6)
recipes = {}

for iid, input_dict in recipe_input_dict.items():
    input_urls = input_dict['urls']

    pattern = pattern_from_file_sequence(input_urls, concat_dim='time')
    transforms = (
        beam.Create(pattern.items())
        | OpenURLWithFSSpec()
        | OpenWithXarray(xarray_open_kwargs={'use_cftime':True}) # do not specify file type to accomdate both ncdf3 and ncdf4
        | StoreToZarr(
            # store_name=f"{iid}.zarr",
            combine_dims=pattern.combine_dim_keys,
            target_chunk_nbytes=target_chunk_nbytes,
            chunk_dim=pattern.concat_dims[0] # not sure if this is better than hardcoding?
        )
    )
    recipes[iid] = transforms
