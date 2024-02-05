[![DOI](https://zenodo.org/badge/618127503.svg)](https://zenodo.org/badge/latestdoi/618127503)

# CMIP6-LEAP-feedstock
This repository contains the [pangeo-forge](https://pangeo-forge.org) feedstock for the continued management of the [Pangeo Analysis-Ready Cloud-Optimized CMIP6 data](https://pangeo-data.github.io/pangeo-cmip6-cloud/). The repo is similar to [data-management](https://github.com/leap-stc/data-management) but due to the sheer size of the CMIP archive, we chose to keep this feedstock separate to enable custom solutions and fast development not necessary for other data ingestion recipes.

## How can I request new data?
To request new data, please create a list of instance_ids as strings which is copy-pasteable to python like this:
```python
'CMIP6.CMIP.THU.CIESM.piControl.r1i1p1f1.Omon.uo.gn.v20200220',
...
'CMIP6.CMIP.THU.CIESM.piControl.r1i1p1f1.Omon.vo.gn.v20200220',
'CMIP6.CMIP.CNRM-CERFACS.CNRM-CM6-1-HR.historical.r1i1p1f2.Omon.so.gn.v20191021',
```
You can use [pangeo-forge-esgf](https://github.com/jbusecke/pangeo-forge-esgf#parsing-a-list-of-instance-ids-using-wildcards) to make your life easier here.

Equiped with that list, please open a [request issue](https://github.com/leap-stc/cmip6-leap-feedstock/issues/new/choose) on this repo and follow the instructions.

> ☺️ Setting expectations: This process is still highly experimental and we thus cannot give any promise regarding the processing of your request.
> We are very keen to enable as much science as possible here, and thus try to work on urgent requests (e.g. for a publication to be submitted soon)
> first, so if please make sure to mention a situation like that in the issue.


## How to access the newly uploaded data?

We are very excited to announce the new beta for the `Pangeo-ESGF CMIP6 Zarr 2.0` public data 🎉

You can access the data in a similar way to the [legacy CMIP6 zarr catalog](https://pangeo-data.github.io/pangeo-cmip6-cloud/accessing_data.html#loading-an-esm-collection) by choosing between these three catalogs:
- The `main` catalog `"catalog.json"` which contains datasets that pass our test and are not retracted.
    - ✨ These datasets should be fully ready for publications
- The `no-QC` catalog `"catalog_noqc.json"`: Some datasets are successfully written to zarr stores, but do not pass our [quality control tests](https://github.com/leap-stc/cmip6-leap-feedstock/blob/9e6290ed2c29a8da93285aeffaea0b639dca79eb/feedstock/recipe.py#L188-L235). You can inspect these datasets in the same way as the quality controlled ones, but the datasets might contain issues like gaps in time, missing attributes etc. Some of these might be fixable, but will require manual work. If you believe that a dataset in this catalog is fixable and should be added to the main catalog, please report the fix in our [issue tracker](https://github.com/leap-stc/cmip6-leap-feedstock/issues/new).
    -  ⚠️ Many of these datasets are probably not fit for publications. Proceed with care when working with these.
- The `retracted` catalog `"catalog_retracted.json"`: These datasets have been officially retracted by ESGF. We retain all ingested stores to enable researchers to quantify changes due to a retraction. You can access all datasets that have been ingested and later retracted via this catalog.
    - ☠️ None of these datasets should be used in publications

```python
import intake
# uncomment/comment lines to swap catalogs
url = "https://storage.googleapis.com/cmip6/cmip6-pgf-ingestion-test/catalog/catalog.json" # Only stores that pass current tests
# url = "https://storage.googleapis.com/cmip6/cmip6-pgf-ingestion-test/catalog/catalog_noqc.json" # Only stores that fail current tests
# url = "https://storage.googleapis.com/cmip6/cmip6-pgf-ingestion-test/catalog/catalog_retracted.json" # Only stores that have been retracted by ESGF
col = intake.open_esm_datastore(url)
```

> [!WARNING]  
> **Expect changes** We are thankful to you for testing the beta version of this catalog but please keep in mind that things can change rapidly (e.g. stores can be moved from one catalog to another) and prepare accordingly. Please check for progress towards the final release [here](https://github.com/leap-stc/cmip6-leap-feedstock/issues/82) .

You can then perform the same operations as with the legacy catalog (please check out the [official docs](https://pangeo-data.github.io/pangeo-cmip6-cloud/accessing_data.html#loading-an-esm-collection) for more info). 

> [!NOTE]
> Some facet values were renamed to follow the ESGF convention. `'member_id'` is now `'variant_label'` and `'dcpp_init_year'` is now `'sub_experiment_id'`. As described in the [CMIP6 global attributes and filenames controlled vocabulary](https://docs.google.com/document/d/1h0r8RZr_f3-8egBMMh7aqLwy3snpD6_MrDz1q8n5XUk/edit) the faced `'member_id'` is now "a compound construction from sub_experiment_id and variant_label". In most cases `variant_label = member_id`, but if `"sub_experiment_id"` is not none, `member_id = <sub_experiment_id>-<variant_label>`.

Also please consider using [xMIP](https://github.com/jbusecke/xMIP) to preprocess the data and take care of common data cleaning tasks.

```python
cat = col.search(variable_id='pr', experiment_id='historical')

from xmip.preprocessing import combined_preprocessing
ddict = cat.to_dataset_dict(preprocess=combined_preprocessing)
```

## Troubleshooting

### I have found an issue with one of the cloud zarr stores. Where can I report this?
Reporting issues is a vital part of this community work, and if you are reading this, I want to thank you for taking the time to do so!

The first step is identifying the type of error, which will determine where to report the error properly. 
Here are a few steps to triage the error. 

Assuming you are loading the data as instructed above using [intake-esm](https://github.com/intake/intake-esm) and you encounter an issue with the data:
1. Check if the Problem dissapears when you do not use xmip (omit `preprocess=combined_preprocessing` above). If that fixes the problem, raise an [issue in the xMIP repo](https://github.com/jbusecke/xMIP/issues/new)
2. Check if the problem disspears when you load the raw zarr store. You can do so by inspecting the `zstore` column of the pandas dataframe underlying the intake-esm collection:
    ```python
    display(cat.df)
    print(cat.df['zstore'].tolist())
    # you can then open each of the urls like this
    import xarray as xr
    ds_test = xr.open_dataset(url, engine='zarr', chunks={})
    ```
    If this solves your problem, you should head over to intake-esm and check the [discussion topics](https://github.com/intake/intake-esm/discussions) and [issues](https://github.com/intake/intake-esm/issues) and raise either one if appropriate.
3. If your error persists, this is either related to the ingestion here or is an error in the original ESGF data. Please raise an issue [right here](https://github.com/leap-stc/cmip6-leap-feedstock/issues/new?assignees=&labels=bug&projects=&template=problem.yaml&title=%5BBUG%5D%3A+) and we will get to the bottom of it.

Thanks for helping to improve everyones experience with CMIP6 data! 

![](https://media.giphy.com/media/p0xvfeVhS7tlhGzIoh/giphy-downsized-large.gif)


## How to run recipes locally (with PGF runner)
- Make sure to set up the environment (TODO: Add this as docs on pangeo-forge-runner)
- Create a scratch dir (e.g. on the desktop it should not be within a repo)
- call  pfg with a local path `pangeo-forge-runner bake --repo path_to_repo -f path_to_config.json`
- data will be generated in this (scratch) dir.
> Example call: `pangeo-forge-runner bake --repo=/Users/juliusbusecke/Code/CMIP6-LEAP-feedstock -f /Users/juliusbusecke/Code/CMIP6-LEAP-feedstock/configs/config_local.json --Bake.job_name=cmip6test`
- TODO: In pgf-runner error if all the storage locations are not just an abstract filestystem
- From charles: install pgf recipes locally with editable flag
  - Get a debugger running within the pgf code (TODO: ask charles again how to do ti.
  )

### Dev Guide

- Set up a local conda environment with `mamba env create -f environment.yml`
- Make sure to update this (`mamba env update -f environment.yml`) when any changes are made e.g. in `feedstock/requirements.txt` ``

### How to develop

- I find it super irritating how hard it is to develop recipes locally. When I run into trouble with running PGF-runnner locally i have to rewrite my whole recipe (add target_root etc). Is there a better way to do this? Some bare bones debug call to PGF runner?

#### Common pitfalls

- Dependencies: This is quite the minefield. Here are some common things and the solutions:
  - `unpickled = cloudpickle.loads(s);ModuleNotFoundError: No module named '__builtin__'`: Wrong version of cloudpickle. I solved this by reinstalling beam with `pip install apache-beam[gcp]`
  - Something about 'MetadataCache' not being found: This is a problem with the version of pangeo-forge-recipes or pangeo-forge-runner.
 I locked a working local environment in `environment-local.locked` so I have a ref point for the future.
