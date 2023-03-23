from transforms import StoreToZarrLegacyDynamic as StoreToZarr
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
import apache_beam as beam
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray
from pyesgf.search import SearchConnection

iids = [
'CMIP6.CMIP.BCC.BCC-CSM2-MR.historical.r1i1p1f1.day.pr.gn.v20181126',
'CMIP6.CMIP.BCC.BCC-CSM2-MR.historical.r1i1p1f1.day.sfcWind.gn.v20181126',
'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r2i1p1f1.day.sfcWind.gn.v20210907',
'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r3i1p1f1.day.sfcWind.gn.v20210907',
'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r4i1p1f1.day.sfcWind.gn.v20210907',
'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r5i1p1f1.day.sfcWind.gn.v20210907',
'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r2i1p1f1.day.psl.gn.v20210907',
'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r3i1p1f1.day.psl.gn.v20210907',
'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r4i1p1f1.day.psl.gn.v20210907',
'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r5i1p1f1.day.psl.gn.v20210907',
'CMIP6.ScenarioMIP.MIROC.MIROC6.ssp585.r2i1p1f1.day.psl.gn.v20200623',
'CMIP6.ScenarioMIP.MIROC.MIROC6.ssp585.r3i1p1f1.day.psl.gn.v20200623', 
'CMIP6.ScenarioMIP.MIROC.MIROC6.ssp585.r4i1p1f1.day.psl.gn.v20200623', 
'CMIP6.ScenarioMIP.MIROC.MIROC6.ssp585.r5i1p1f1.day.psl.gn.v20200623', 
'CMIP6.ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp585.r6i1p1f1.day.sfcWind.gr.v20200201', 
'CMIP6.ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp585.r9i1p1f1.day.sfcWind.gr.v20200201', 
'CMIP6.ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp585.r11i1p1f1.day.sfcWind.gr.v20200201', 
'CMIP6.ScenarioMIP.MOHC.HadGEM3-GC31-MM.ssp585.r2i1p1f3.day.psl.gn.v20200515',
'CMIP6.ScenarioMIP.MOHC.HadGEM3-GC31-MM.ssp585.r3i1p1f3.day.psl.gn.v20200507',
]


## Query ESGF for the urls
iid_schema = 'mip_era.activity_id.institution_id.source_id.experiment_id.member_id.table_id.variable_id.grid_label.version'

conn = SearchConnection(
    "https://esgf-node.llnl.gov/esg-search",
    distrib=True
)
recipe_input_dict = {}
for iid in iids:
    context_kwargs = {'replica':None,'facets':['doi']} # this assumes that I can use replicas just as master records. I think that is fine
    for label, value in zip(iid_schema.split('.'), iid.split('.')):
        context_kwargs[label] = value
        
    # is this a problem with the `v...` in version?
    context_kwargs['version'] = context_kwargs['version'].replace('v','')
        
    # testing
    # del context_kwargs['version']
    ctx = conn.new_context(**context_kwargs)
    print(f"{iid}: Found {ctx.hit_count} hits")
    
    results = ctx.search() # these might include several data nodes (curiously even if I set replica to false?)
    
    if len(results)<1:
        print(f'Nothing found for {iid}. Skipping.')
    else:
        # for now take the first one that has any urls at all
        for ri,result in enumerate(results):
            print(f"{iid}-{result.dataset_id}: Extracting URLS")
            urls = [f.download_url for f in result.file_context().search()] # this raises an annoying warning each time. FIXME
            if len(urls)>0:
                recipe_input_dict[iid] = {}
                recipe_input_dict[iid]['urls'] = urls
                # populate this to pass to the database later
                recipe_input_dict[iid]['instance_id'],recipe_input_dict[iid]['data_node'] = result.dataset_id.split('|')
                break

# create recipe dictionary
target_chunk_nbytes = int(100e6)
recipes = {}

for iid, input_dict in recipe_input_dict.items():
    input_urls = input_dict['urls']

    pattern = pattern_from_file_sequence(input_urls, concat_dim='time')
    transforms = (
        beam.Create(pattern.items())
        | OpenURLWithFSSpec()
        | OpenWithXarray() # do not specify file type to accomdate both ncdf3 and ncdf4
        | StoreToZarrLegacyDynamic(
            store_name=f"{iid}.zarr",
            combine_dims=pattern.combine_dim_keys,
            target_chunk_nbytes=target_chunk_nbytes,
            chunk_dim=pattern.concat_dims[0] # not sure if this is better than hardcoding?
        )
    )
    recipes[iid] = transforms
