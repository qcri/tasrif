# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: PyCharm (tasrif)
#     language: python
#     name: pycharm-5bd30262
# ---

# %%
# %load_ext autoreload
# %autoreload 2

import numpy as np

from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SortOperator, ReplaceOperator
from tasrif.processing_pipeline.pandas import DropDuplicatesOperator, DropNAOperator, DropFeaturesOperator
from tasrif.processing_pipeline.custom import OneHotEncoderOperator

from tasrif.data_readers.sleep_health import MyFamilyDataset


# %%
def col_stats(df):
    print('Some important stats:')
    print('\t- This dataset contains', len(df) ,' rows.')
    for col in df.columns:
        null_percentage = "{:.2f}".format(df[col].isnull().sum()/len(df)*100)
        print('\t - ``', col, '`` has', df[col].isnull().sum(), 'NAs (', df[col].count().sum(), '/', len(df), ') =',
              null_percentage, '%')


# %% pycharm={"name": "#%%\n"}
# Full MyFamilyDataset
mf = MyFamilyDataset(shc_folder="../../data/sleephealth/", pipeline=None)
df = mf.raw_df.copy()
print("Shape:", df.shape)
df.head()

# %% pycharm={"name": "#%%\n"}
col_stats(df)

# %% pycharm={"name": "#%%\n"}
# Default Pipeline SleepQualityCheckerDataset
pipeline = ProcessingPipeline([
    ConvertToDatetimeOperator(feature_names="timestamp", format="%Y-%m-%dT%H:%M:%S%z", utc=True),
    SortOperator(by=["participantId", "timestamp"]),
    DropDuplicatesOperator(subset="participantId", keep="last"),
    ReplaceOperator(to_replace={"fam_history": {"200": np.nan},
                                "family_size": {6: np.nan},
                                "language": {5: np.nan},
                                "underage_family": {6: np.nan}}),
    DropNAOperator(subset=["fam_history", "family_size", "language", "underage_family"]),
    OneHotEncoderOperator(feature_names=["fam_history", "family_size", "language", "underage_family"], drop_first=False),
    DropFeaturesOperator(["fam_history=200"])
    ])

mypipe = MyFamilyDataset(shc_folder="../../data/sleephealth/", pipeline=pipeline)
df_piped = mypipe.processed_dataframe()
print("Shape:", df_piped.shape)
df_piped.head()
