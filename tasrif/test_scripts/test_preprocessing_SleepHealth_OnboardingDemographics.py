# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2
import os
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.pandas import ReplaceOperator, DropNAOperator
from tasrif.data_readers.sleep_health import OnboardingDemographicsDataset
import pandas as pd
import numpy as np

# %%
# Full AboutMeDataset
obd = OnboardingDemographicsDataset(os.environ['SLEEPHEALTH_ONBOARDINGDEMOGRAPHICS_PATH'], pipeline=None)
df = obd.raw_df.copy()
print("Shape:", df.shape)

# %% pycharm={"name": "#%%\n"}
for key in df:
    nas = df[key].isna().sum()
    print("# NA rows for col %s: %d (%.2f%%)" % (key, nas, 100.*nas/df.shape[0]))

# %% pycharm={"name": "#%%\n"}
# Analysis to either drop or not censored data
df[df["weight_pounds"] == "CENSORED"] # 431 (5.3%)
df[df["height_inches"] == "CENSORED"] # 337 (4.1%)
df[(df["height_inches"] == "CENSORED") & (df["weight_pounds"] == "CENSORED")] # 254 (3.1%)
df[(df["height_inches"] == "CENSORED") | (df["weight_pounds"] == "CENSORED")] # 514 (6.3%)

# %% pycharm={"name": "#%%\n"}
pipeline = ProcessingPipeline([
    ReplaceOperator(to_replace="CENSORED", value=np.nan),
    DropNAOperator()
    ])

obd = OnboardingDemographicsDataset(os.environ['SLEEPHEALTH_ONBOARDINGDEMOGRAPHICS_PATH'], pipeline=pipeline)
df = obd.processed_dataframe()
df


