# CMIP6-LEAP-feedstock

This repository is similar to [data-management](https://github.com/leap-stc/data-management) but due to the sheer size of the CMIP archive, we chose to keep this feedstock separate to enable custom solutions and fast development not necessary for other data ingestion recipes.

## Dev Guide

- Set up a local conda environment with `mamba env create -f environment.yml`
- Make sure to update this (`mamba env update -f environment.yml`) when any changes are made e.g. in `feedstock/requirements.txt` ``

## How to develop

- I find it super irritating how hard it is to develop recipes locally. When I run into trouble with running PGF-runnner locally i have to rewrite my whole recipe (add target_root etc). Is there a better way to do this? Some bare bones debug call to PGF runner?

### Common pitfalls

- Dependencies: This is quite the minefield. Here are some common things and the solutions:
  - `unpickled = cloudpickle.loads(s);ModuleNotFoundError: No module named '__builtin__'`: Wrong version of cloudpickle. I solved this by reinstalling beam with `pip install apache-beam[gcp]`
  - Something about 'MetadataCache' not being found: This is a problem with the version of pangeo-forge-recipes or pangeo-forge-runner.
 I locked a working local environment in `environment-local.locked` so I have a ref point for the future.
