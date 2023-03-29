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
recipe_input_dict = {'CMIP6.CMIP.BCC.BCC-CSM2-MR.historical.r1i1p1f1.day.pr.gn.v20181126': {'urls': ['http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/pr/gn/v20181126/pr_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_18500101-18741231.nc',
   'http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/pr/gn/v20181126/pr_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_18750101-18991231.nc',
   'http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/pr/gn/v20181126/pr_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_19000101-19241231.nc',
   'http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/pr/gn/v20181126/pr_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_19250101-19491231.nc',
   'http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/pr/gn/v20181126/pr_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_19500101-19741231.nc',
   'http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/pr/gn/v20181126/pr_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_19750101-19991231.nc',
   'http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/pr/gn/v20181126/pr_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_20000101-20141231.nc'],
  'instance_id': 'CMIP6.CMIP.BCC.BCC-CSM2-MR.historical.r1i1p1f1.day.pr.gn.v20181126',
  'data_node': 'esgf.nci.org.au'},
 'CMIP6.CMIP.BCC.BCC-CSM2-MR.historical.r1i1p1f1.day.sfcWind.gn.v20181126': {'urls': ['http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/sfcWind/gn/v20181126/sfcWind_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_18500101-18741231.nc',
   'http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/sfcWind/gn/v20181126/sfcWind_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_18750101-18991231.nc',
   'http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/sfcWind/gn/v20181126/sfcWind_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_19000101-19241231.nc',
   'http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/sfcWind/gn/v20181126/sfcWind_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_19250101-19491231.nc',
   'http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/sfcWind/gn/v20181126/sfcWind_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_19500101-19741231.nc',
   'http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/sfcWind/gn/v20181126/sfcWind_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_19750101-19991231.nc',
   'http://esgf.nci.org.au/thredds/fileServer/replica/CMIP6/CMIP/BCC/BCC-CSM2-MR/historical/r1i1p1f1/day/sfcWind/gn/v20181126/sfcWind_day_BCC-CSM2-MR_historical_r1i1p1f1_gn_20000101-20141231.nc'],
  'instance_id': 'CMIP6.CMIP.BCC.BCC-CSM2-MR.historical.r1i1p1f1.day.sfcWind.gn.v20181126',
  'data_node': 'esgf.nci.org.au'},
 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r2i1p1f1.day.sfcWind.gn.v20210907': {'urls': ['https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r2i1p1f1/day/sfcWind/gn/v20210907/sfcWind_day_MRI-ESM2-0_ssp585_r2i1p1f1_gn_20150101-20641231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r2i1p1f1/day/sfcWind/gn/v20210907/sfcWind_day_MRI-ESM2-0_ssp585_r2i1p1f1_gn_20650101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r2i1p1f1.day.sfcWind.gn.v20210907',
  'data_node': 'esgf-data1.llnl.gov'},
 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r3i1p1f1.day.sfcWind.gn.v20210907': {'urls': ['http://esgf-data03.diasjp.net/thredds/fileServer/esg_dataroot/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r3i1p1f1/day/sfcWind/gn/v20210907/sfcWind_day_MRI-ESM2-0_ssp585_r3i1p1f1_gn_20150101-20641231.nc',
   'http://esgf-data03.diasjp.net/thredds/fileServer/esg_dataroot/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r3i1p1f1/day/sfcWind/gn/v20210907/sfcWind_day_MRI-ESM2-0_ssp585_r3i1p1f1_gn_20650101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r3i1p1f1.day.sfcWind.gn.v20210907',
  'data_node': 'esgf-data03.diasjp.net'},
 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r4i1p1f1.day.sfcWind.gn.v20210907': {'urls': ['https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r4i1p1f1/day/sfcWind/gn/v20210907/sfcWind_day_MRI-ESM2-0_ssp585_r4i1p1f1_gn_20150101-20641231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r4i1p1f1/day/sfcWind/gn/v20210907/sfcWind_day_MRI-ESM2-0_ssp585_r4i1p1f1_gn_20650101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r4i1p1f1.day.sfcWind.gn.v20210907',
  'data_node': 'esgf-data1.llnl.gov'},
 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r5i1p1f1.day.sfcWind.gn.v20210907': {'urls': ['http://esgf-data03.diasjp.net/thredds/fileServer/esg_dataroot/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r5i1p1f1/day/sfcWind/gn/v20210907/sfcWind_day_MRI-ESM2-0_ssp585_r5i1p1f1_gn_20150101-20641231.nc',
   'http://esgf-data03.diasjp.net/thredds/fileServer/esg_dataroot/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r5i1p1f1/day/sfcWind/gn/v20210907/sfcWind_day_MRI-ESM2-0_ssp585_r5i1p1f1_gn_20650101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r5i1p1f1.day.sfcWind.gn.v20210907',
  'data_node': 'esgf-data03.diasjp.net'},
 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r2i1p1f1.day.psl.gn.v20210907': {'urls': ['https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r2i1p1f1/day/psl/gn/v20210907/psl_day_MRI-ESM2-0_ssp585_r2i1p1f1_gn_20150101-20641231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r2i1p1f1/day/psl/gn/v20210907/psl_day_MRI-ESM2-0_ssp585_r2i1p1f1_gn_20650101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r2i1p1f1.day.psl.gn.v20210907',
  'data_node': 'esgf-data1.llnl.gov'},
 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r3i1p1f1.day.psl.gn.v20210907': {'urls': ['http://esgf-data03.diasjp.net/thredds/fileServer/esg_dataroot/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r3i1p1f1/day/psl/gn/v20210907/psl_day_MRI-ESM2-0_ssp585_r3i1p1f1_gn_20150101-20641231.nc',
   'http://esgf-data03.diasjp.net/thredds/fileServer/esg_dataroot/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r3i1p1f1/day/psl/gn/v20210907/psl_day_MRI-ESM2-0_ssp585_r3i1p1f1_gn_20650101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r3i1p1f1.day.psl.gn.v20210907',
  'data_node': 'esgf-data03.diasjp.net'},
 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r4i1p1f1.day.psl.gn.v20210907': {'urls': ['https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r4i1p1f1/day/psl/gn/v20210907/psl_day_MRI-ESM2-0_ssp585_r4i1p1f1_gn_20150101-20641231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r4i1p1f1/day/psl/gn/v20210907/psl_day_MRI-ESM2-0_ssp585_r4i1p1f1_gn_20650101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r4i1p1f1.day.psl.gn.v20210907',
  'data_node': 'esgf-data1.llnl.gov'},
 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r5i1p1f1.day.psl.gn.v20210907': {'urls': ['http://esgf-data03.diasjp.net/thredds/fileServer/esg_dataroot/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r5i1p1f1/day/psl/gn/v20210907/psl_day_MRI-ESM2-0_ssp585_r5i1p1f1_gn_20150101-20641231.nc',
   'http://esgf-data03.diasjp.net/thredds/fileServer/esg_dataroot/CMIP6/ScenarioMIP/MRI/MRI-ESM2-0/ssp585/r5i1p1f1/day/psl/gn/v20210907/psl_day_MRI-ESM2-0_ssp585_r5i1p1f1_gn_20650101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.MRI.MRI-ESM2-0.ssp585.r5i1p1f1.day.psl.gn.v20210907',
  'data_node': 'esgf-data03.diasjp.net'},
 'CMIP6.ScenarioMIP.MIROC.MIROC6.ssp585.r4i1p1f1.day.psl.gn.v20200623': {'urls': ['https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r4i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r4i1p1f1_gn_20150101-20241231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r4i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r4i1p1f1_gn_20250101-20341231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r4i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r4i1p1f1_gn_20350101-20441231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r4i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r4i1p1f1_gn_20450101-20541231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r4i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r4i1p1f1_gn_20550101-20641231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r4i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r4i1p1f1_gn_20650101-20741231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r4i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r4i1p1f1_gn_20750101-20841231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r4i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r4i1p1f1_gn_20850101-20941231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r4i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r4i1p1f1_gn_20950101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.MIROC.MIROC6.ssp585.r4i1p1f1.day.psl.gn.v20200623',
  'data_node': 'esgf-data1.llnl.gov'},
 'CMIP6.ScenarioMIP.MIROC.MIROC6.ssp585.r5i1p1f1.day.psl.gn.v20200623': {'urls': ['https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r5i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r5i1p1f1_gn_20150101-20241231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r5i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r5i1p1f1_gn_20250101-20341231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r5i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r5i1p1f1_gn_20350101-20441231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r5i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r5i1p1f1_gn_20450101-20541231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r5i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r5i1p1f1_gn_20550101-20641231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r5i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r5i1p1f1_gn_20650101-20741231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r5i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r5i1p1f1_gn_20750101-20841231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r5i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r5i1p1f1_gn_20850101-20941231.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC6/ssp585/r5i1p1f1/day/psl/gn/v20200623/psl_day_MIROC6_ssp585_r5i1p1f1_gn_20950101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.MIROC.MIROC6.ssp585.r5i1p1f1.day.psl.gn.v20200623',
  'data_node': 'esgf-data1.llnl.gov'},
 'CMIP6.ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp585.r6i1p1f1.day.sfcWind.gr.v20200201': {'urls': ['https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20150101-20151231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20160101-20161231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20170101-20171231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20180101-20181231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20190101-20191231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20200101-20201231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20210101-20211231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20220101-20221231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20230101-20231231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20240101-20241231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20250101-20251231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20260101-20261231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20270101-20271231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20280101-20281231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20290101-20291231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20300101-20301231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20310101-20311231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20320101-20321231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20330101-20331231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20340101-20341231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20350101-20351231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20360101-20361231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20370101-20371231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20380101-20381231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20390101-20391231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20400101-20401231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20410101-20411231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20420101-20421231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20430101-20431231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20440101-20441231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20450101-20451231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20460101-20461231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20470101-20471231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20480101-20481231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20490101-20491231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20500101-20501231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20510101-20511231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20520101-20521231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20530101-20531231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20540101-20541231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20550101-20551231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20560101-20561231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20570101-20571231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20580101-20581231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20590101-20591231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20600101-20601231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20610101-20611231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20620101-20621231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20630101-20631231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20640101-20641231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20650101-20651231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20660101-20661231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20670101-20671231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20680101-20681231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20690101-20691231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20700101-20701231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20710101-20711231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20720101-20721231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20730101-20731231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20740101-20741231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20750101-20751231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20760101-20761231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20770101-20771231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20780101-20781231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20790101-20791231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20800101-20801231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20810101-20811231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20820101-20821231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20830101-20831231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20840101-20841231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20850101-20851231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20860101-20861231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20870101-20871231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20880101-20881231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20890101-20891231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20900101-20901231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20910101-20911231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20920101-20921231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20930101-20931231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20940101-20941231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20950101-20951231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20960101-20961231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20970101-20971231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20980101-20981231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_20990101-20991231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r6i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r6i1p1f1_gr_21000101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp585.r6i1p1f1.day.sfcWind.gr.v20200201',
  'data_node': 'esgf.ichec.ie'},
 'CMIP6.ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp585.r9i1p1f1.day.sfcWind.gr.v20200201': {'urls': ['https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20150101-20151231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20160101-20161231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20170101-20171231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20180101-20181231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20190101-20191231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20200101-20201231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20210101-20211231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20220101-20221231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20230101-20231231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20240101-20241231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20250101-20251231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20260101-20261231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20270101-20271231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20280101-20281231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20290101-20291231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20300101-20301231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20310101-20311231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20320101-20321231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20330101-20331231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20340101-20341231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20350101-20351231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20360101-20361231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20370101-20371231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20380101-20381231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20390101-20391231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20400101-20401231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20410101-20411231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20420101-20421231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20430101-20431231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20440101-20441231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20450101-20451231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20460101-20461231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20470101-20471231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20480101-20481231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20490101-20491231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20500101-20501231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20510101-20511231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20520101-20521231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20530101-20531231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20540101-20541231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20550101-20551231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20560101-20561231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20570101-20571231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20580101-20581231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20590101-20591231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20600101-20601231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20610101-20611231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20620101-20621231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20630101-20631231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20640101-20641231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20650101-20651231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20660101-20661231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20670101-20671231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20680101-20681231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20690101-20691231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20700101-20701231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20710101-20711231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20720101-20721231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20730101-20731231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20740101-20741231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20750101-20751231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20760101-20761231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20770101-20771231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20780101-20781231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20790101-20791231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20800101-20801231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20810101-20811231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20820101-20821231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20830101-20831231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20840101-20841231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20850101-20851231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20860101-20861231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20870101-20871231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20880101-20881231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20890101-20891231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20900101-20901231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20910101-20911231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20920101-20921231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20930101-20931231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20940101-20941231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20950101-20951231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20960101-20961231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20970101-20971231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20980101-20981231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_20990101-20991231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r9i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r9i1p1f1_gr_21000101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp585.r9i1p1f1.day.sfcWind.gr.v20200201',
  'data_node': 'esgf.ichec.ie'},
 'CMIP6.ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp585.r11i1p1f1.day.sfcWind.gr.v20200201': {'urls': ['https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20150101-20151231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20160101-20161231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20170101-20171231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20180101-20181231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20190101-20191231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20200101-20201231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20210101-20211231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20220101-20221231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20230101-20231231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20240101-20241231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20250101-20251231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20260101-20261231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20270101-20271231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20280101-20281231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20290101-20291231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20300101-20301231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20310101-20311231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20320101-20321231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20330101-20331231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20340101-20341231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20350101-20351231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20360101-20361231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20370101-20371231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20380101-20381231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20390101-20391231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20400101-20401231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20410101-20411231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20420101-20421231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20430101-20431231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20440101-20441231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20450101-20451231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20460101-20461231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20470101-20471231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20480101-20481231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20490101-20491231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20500101-20501231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20510101-20511231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20520101-20521231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20530101-20531231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20540101-20541231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20550101-20551231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20560101-20561231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20570101-20571231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20580101-20581231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20590101-20591231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20600101-20601231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20610101-20611231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20620101-20621231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20630101-20631231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20640101-20641231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20650101-20651231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20660101-20661231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20670101-20671231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20680101-20681231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20690101-20691231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20700101-20701231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20710101-20711231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20720101-20721231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20730101-20731231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20740101-20741231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20750101-20751231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20760101-20761231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20770101-20771231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20780101-20781231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20790101-20791231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20800101-20801231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20810101-20811231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20820101-20821231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20830101-20831231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20840101-20841231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20850101-20851231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20860101-20861231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20870101-20871231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20880101-20881231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20890101-20891231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20900101-20901231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20910101-20911231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20920101-20921231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20930101-20931231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20940101-20941231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20950101-20951231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20960101-20961231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20970101-20971231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20980101-20981231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_20990101-20991231.nc',
   'https://esgf.ichec.ie/thredds/fileServer/cmip6/ScenarioMIP/EC-Earth-Consortium/EC-Earth3/ssp585/r11i1p1f1/day/sfcWind/gr/v20200201/sfcWind_day_EC-Earth3_ssp585_r11i1p1f1_gr_21000101-21001231.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp585.r11i1p1f1.day.sfcWind.gr.v20200201',
  'data_node': 'esgf.ichec.ie'},
 'CMIP6.ScenarioMIP.MOHC.HadGEM3-GC31-MM.ssp585.r2i1p1f3.day.psl.gn.v20200515': {'urls': ['https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r2i1p1f3/day/psl/gn/v20200515/psl_day_HadGEM3-GC31-MM_ssp585_r2i1p1f3_gn_20750101-20791230.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r2i1p1f3/day/psl/gn/v20200515/psl_day_HadGEM3-GC31-MM_ssp585_r2i1p1f3_gn_20800101-20841230.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r2i1p1f3/day/psl/gn/v20200515/psl_day_HadGEM3-GC31-MM_ssp585_r2i1p1f3_gn_20850101-20891230.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r2i1p1f3/day/psl/gn/v20200515/psl_day_HadGEM3-GC31-MM_ssp585_r2i1p1f3_gn_20900101-20941230.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r2i1p1f3/day/psl/gn/v20200515/psl_day_HadGEM3-GC31-MM_ssp585_r2i1p1f3_gn_20950101-20991230.nc',
   'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r2i1p1f3/day/psl/gn/v20200515/psl_day_HadGEM3-GC31-MM_ssp585_r2i1p1f3_gn_21000101-21001230.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.MOHC.HadGEM3-GC31-MM.ssp585.r2i1p1f3.day.psl.gn.v20200515',
  'data_node': 'esgf-data1.llnl.gov'},
 'CMIP6.ScenarioMIP.MOHC.HadGEM3-GC31-MM.ssp585.r3i1p1f3.day.psl.gn.v20200507,': {'urls': ['https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20150101-20191230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20200101-20241230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20250101-20291230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20300101-20341230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20350101-20391230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20400101-20441230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20450101-20491230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20500101-20541230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20550101-20591230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20600101-20641230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20650101-20691230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20700101-20741230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20750101-20791230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20800101-20841230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20850101-20891230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20900101-20941230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_20950101-20991230.nc',
   'https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-MM/ssp585/r3i1p1f3/day/psl/gn/v20200507/psl_day_HadGEM3-GC31-MM_ssp585_r3i1p1f3_gn_21000101-21001230.nc'],
  'instance_id': 'CMIP6.ScenarioMIP.MOHC.HadGEM3-GC31-MM.ssp585.r3i1p1f3.day.psl.gn.v20200507',
  'data_node': 'esgf.ceda.ac.uk'}}
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
