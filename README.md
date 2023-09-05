# CMIP6-LEAP-feedstock

This repository is similar to [data-management](https://github.com/leap-stc/data-management) but due to the sheer size of the CMIP archive, we chose to keep this feedstock separate to enable custom solutions and fast development not necessary for other data ingestion recipes.

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
The cataloging and uploading are very much a work in progress.
For the moment you can access the data separately from the [main CMIP6 zarr catalog](https://pangeo-data.github.io/pangeo-cmip6-cloud/accessing_data.html#loading-an-esm-collection) by using the following catalog:

```python
import intake
col = intake.open_esm_datastore(
    "https://storage.googleapis.com/leap-persistent-ro/data-library/catalogs/cmip6-test/leap-pangeo-cmip6-test.json"
)
cat = col.search(variable_id='pr', experiment_id='historical')
```
You can then perform the same operations as with the main catalog.

### Non quality-controlled catalog (⛔️use with caution⛔️)
Some datasets are successfully written to zarr stores, but do not pass our [quality control tests](https://github.com/leap-stc/cmip6-leap-feedstock/blob/9e6290ed2c29a8da93285aeffaea0b639dca79eb/feedstock/recipe.py#L188-L235). You can inspect these datasets in the same way as the quality controlled ones, but the datasets might contain issues like gaps in time, missing attributes etc. Some of these might be fixable, but will require manual work that is for now out of scope of this project.
Be very careful when using these datasets, in particular for publications!
```python
import intake
col = intake.open_esm_datastore(
    "https://storage.googleapis.com/leap-persistent-ro/data-library/catalogs/cmip6-test/leap-pangeo-cmip6-noqc-test.json"
)
cat = col.search(variable_id='pr', experiment_id='historical')
```

### How to run recipes locally (with PGF runner)
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
