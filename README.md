[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/leap-stc/cmip6-leap-feedstock/main.svg)](https://results.pre-commit.ci/latest/github/leap-stc/cmip6-leap-feedstock/main)
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

We are very excited to announce the new beta for the `Pangeo-ESGF CMIP6 Zarr Data 2.0` 🎉

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

## I filed a request and want to check progress. How do I do that?

We provide a script to monitor a given request. Clone this repo, install [uv](https://docs.astral.sh/uv/) (if not already installed) and create a file with all the requested instance ids as a simple text file (You can use the helper scripts `scripts/monitoring/convert_yaml_to_list.py` if needed) and run

```
uv run python scripts/monitoring/search_iids_from_file.py <path/to/your/request_file.txt> --count-only
```
The `--count-only` flag gives a short summary, omit if you want to see the list of actual instance_ids.

You can also provide a search string which will display the count of full list of instance ids that contain the string.
```
uv run python scripts/monitoring/search_iids_from_file.py <path/to/your/request_file.txt> --count-only --search-string=".historical."
```
this can be useful if you know that there is an issue with e.g. a particular model, experiment etc and you want to compare missing iids with known failures.


## What do you actually do to the data?
The goal of this feedstock is to make CMIP6 data analysis-ready, but not modify the source data in any way.

The current workflow involves the following steps for every instance id:
- Query the ESGF API using [pangeo-forge-esgf](https://github.com/jbusecke/pangeo-forge-esgf) to get a list of urls that represent all files for a single dataset
- [Preprocess](https://github.com/leap-stc/cmip6-leap-feedstock/blob/32041e50485448505182172faf854e607df0606d/feedstock/recipe.py#L35-L70) each dataset resulting from a file. This step sanitizes some attribute formatting and moves additional variables to the coordinates of the dataset. This does **not alter** the data or naming in any way.
- Combine datasets along the time dimension.
- [Dynamically rechunk](https://github.com/leap-stc/cmip6-leap-feedstock/blob/32041e50485448505182172faf854e607df0606d/feedstock/recipe.py#L245-L317) and store the data to zarr.
- Consolidate the metadata and dimension coordinates of the zarr store.

None of these steps attempt to correct any naming issues ([xMIP](https://github.com/jbusecke/xMIP) can do this after you load the data), or at any point modify the data itself! The xarray datasets you load from zarr should give you the same data you would get if you download the netcdf files and open them with xarray locally!


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

### My iid does not get submitted. What is wrong?
Could be a bunch of reasons, but lets go through some debugging together. Ok first lets check if you get any response for a given iid:
```python
from pangeo_forge_esgf import get_urls_from_esgf, setup_logging
setup_logging('DEBUG')
iids = ['something.that.doesnt.ingest.well']
url_dict = await get_urls_from_esgf(iids)
```
This might give you some useful error messages and will tell you if the issue is parsing urls from the ESGF API (if the url_dict is empty) or if the problems arise when the urls are passed to pangeo-forge-recipes.

If not we need to dig deeper... Coming soon.

## How many datasets have been ingested by LEAP?
This little snippet can be used to identify how many datasets have been ingested during the second phase (fully based on pangeo-forge):
```python
import intake

def count_new_iids(col_url:str):
    col = intake.open_esm_datastore(col_url)
    prefix = [p.replace('gs://cmip6/','').split('/')[0] for p in col.df['zstore'].tolist()]
    new_iids = [p for p in prefix if p in ['CMIP6_LEAP_legacy','cmip6-pgf-ingestion-test']]
    return len(new_iids)

url_dict = {
    'qc':"https://storage.googleapis.com/cmip6/cmip6-pgf-ingestion-test/catalog/catalog.json",
    'non-qc':"https://storage.googleapis.com/cmip6/cmip6-pgf-ingestion-test/catalog/catalog_noqc.json",
    'retracted':"https://storage.googleapis.com/cmip6/cmip6-pgf-ingestion-test/catalog/catalog_retracted.json"
}

iids_found = []
for catalog,url in url_dict.items():

    n_new_iids = count_new_iids(url)
    print(f"{url=} LEAP ingested datasets {n_new_iids}")
```
Last this was updated we ingested over 5000 datasets already!

## Dev Guide

### Setting up environment

>[!WARN]
> I am trying to refactor this repo to use uv, but have not yet fully migrated the feedstock dependencies.
> For now keep using mamba here.

Set up a local development environment
```
mamba create -n cmip6-feedstock python=3.11 -y
conda activate cmip6-feedstock
pip install pangeo-forge-runner==0.10.2 --no-cache-dir
pip install -r feedstock/requirements.txt
```

### Debug recipe locally
It can be handy to debug the recipe creation locally to shorten the iteration cycle (which is long when every change kicks off a gh deploy action).


#### Expanding metadata
This will generate the recipes but not actually execute them.

Assuming you have pangeo-forge-runner installed **and are authenticated for Google Cloud (big query access**)** you should be able to do this
```
export IS_TEST=true; \
export GITHUB_RUN_ID=a; \
export GITHUB_RUN_ATTEMPT=bb; \
export GOOGLE_CLOUD_PROJECT=leap-pangeo; \
pangeo-forge-runner expand-meta --repo=.
```

This can be very handy to detect issues with the ESGF API (or pangeo-forge-esgf)

`IS_TEST` is usually used for a set of test iids in CI, but will do a couple of things here. It will grab iids from feedstock/iids_pr.yaml and provides more detailed logging.
`GITHUB_RUN_ID` and `GITHUB_RUN_ATTEMPT` are simply dummy values that the recipe expects in the environment
`GOOGLE_CLOUD_PROJECT` is needed to properly access the bigquery tables (**NOTE: You will also have to generate [Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc) locally).

#### Executing the recipe
If you want to debug the actual execution of the recipe you need to 'bake' the recipe.

```
export IS_TEST=true; \
export GITHUB_RUN_ID=a; \
export GITHUB_RUN_ATTEMPT=bb; \
export GOOGLE_CLOUD_PROJECT=leap-pangeo; \
pangeo-forge-runner bake --repo=. --config=configs/config_local.json --Bake.job_name=cmip6localtest
```

This will create a folder `local_storage` where the cache and ouput will be placed.


## How to run recipes locally (with PGF runner)
- Create a scratch dir (e.g. on the desktop it should not be within a repo)
- call  pfg with a local path `pangeo-forge-runner bake --repo path_to_repo -f path_to_config.json`
- data will be generated in this (scratch) dir.
> Example call: `pangeo-forge-runner bake --repo=/Users/juliusbusecke/Code/CMIP6-LEAP-feedstock --config /Users/juliusbusecke/Code/CMIP6-LEAP-feedstock/configs/config_local.json --Bake.job_name=cmip6test`
- TODO: In pgf-runner error if all the storage locations are not just an abstract filestystem
- From charles: install pgf recipes locally with editable flag
  - Get a debugger running within the pgf code (TODO: ask charles again how to do ti.
  )

### Dev Guide








pangeo-forge-runner bake \
            --repo=${{ github.server_url }}/${{ github.repository }}.git \
            --ref=${{ github.sha }} \
            --feedstock-subdir='feedstock' \
            --Bake.job_name=${{ env.JOB_NAME }} \
            --Bake.recipe_id=${{ github.event.inputs.recipe_id }} \
            -f configs/config_dataflow.py
in the base repo. If this succeeds these recipes should be submittable (I hope).

### Script Documentation

#### `scripts/dump_bigquery_to_csv.py`
Uses latest entries from bigquery to generate a csv file (which is backwards compatible with the intake catalog). Before writing a new version it renames the current copy of the file to generate a dated backup.
>[!NOTE]
> This is a pretty horrible way to do this and any larger refactor of this should definitely include a way to do this within a single file/database (Apache Iceberg might be a good fit?)

#### `scripts/retraction.py`
Queries the ESGF API, compares retracted datasets to the bigquery table, and adds new entries if there are newly retracted datasets.

#### `scripts/legacy/*`
These are one-off notebooks that were used to re-test and catalog the 'legacy stores' (manually ingested before this pipeline was built).
>[!WARNING]
>These are purely for provenance. Running them again now might result in unexpected outcomes.

#### `scripts/monitoring/convert_yaml_to_list.py`
This script converts a YAML file containing a list into a plain text file, with each item on a new line. This is useful for transforming YAML-formatted lists into a simple line-delimited format that can be easily consumed by command-line tools or other scripts.

#### `scripts/monitoring/search_iids_from_file.py`
This script searches for specified CMIP6 Instance IDs (IIDs) within various CMIP6 intake catalogs. It takes a file containing a list of IIDs as input and reports which IIDs are found in different catalogs (e.g., 'qc', 'non-qc', 'retracted'). It can also provide a count-only summary. This is the primary tool for monitoring the ingestion status of CMIP6 data.

### Jupyter Notebook documentation

TODO: How to use uv to run these notebooks


###
