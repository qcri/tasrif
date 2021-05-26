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
import os
import numpy as np

from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SortOperator, ReplaceOperator
from tasrif.processing_pipeline.pandas import DropDuplicatesOperator, DropNAOperator, DropFeaturesOperator
from tasrif.processing_pipeline.custom import OneHotEncoderOperator

from tasrif.data_readers.sleep_health import MyHealthDataset


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
mf = MyHealthDataset(os.environ['SLEEPHEALTH_MYHEALTH_PATH'], pipeline=None)
df = mf.raw_df.copy()
print("Shape:", df.shape)
df.head()

# %% pycharm={"name": "#%%\n"}
col_stats(df)

# %% pycharm={"name": "#%%\n"}
print("Shape after dropping duplicate participants:", df["participantId"].drop_duplicates().shape)

# %% pycharm={"name": "#%%\n"}
# Default Pipeline
pipeline = ProcessingPipeline([
    ConvertToDatetimeOperator(feature_names="timestamp", format="%Y-%m-%dT%H:%M:%S%z", utc=True),
    SortOperator(by=["participantId", "timestamp"]),
    DropDuplicatesOperator(subset="participantId", keep="last"),
    ReplaceOperator(to_replace={"allergies": {3: np.nan}, "anxiety": {3: np.nan}, "apnea": {3: np.nan},
                                "asthma": {3: np.nan}, "atrial": {3: np.nan}, "hi_blood_pressure": {3: np.nan},
                                "cancer": {3: np.nan}, "depression": {3: np.nan}, "diabetes": {3: np.nan},
                                "erectile": {3: np.nan}, "gastroesophageal": {3: np.nan}, "heart_disease": {3: np.nan},
                                "insomnia": {3: np.nan}, "lung": {3: np.nan}, "narcolepsy": {3: np.nan},
                                "nocturia": {3: np.nan}, "restless_legs_syndrome": {3: np.nan}, "stroke": {3: np.nan},
                                "uars": {3: np.nan}
                               }),
    DropNAOperator(subset=["anxious", "cardiovascular", "compare_one_year", "day_to_day", "depressed", "emotional",
                           "fatigued", "general_health", "mental_health", "physical_activities", "physical_health",
                           "risk", "sleep_trouble", "social_activities", "stressed"]),
    OneHotEncoderOperator(feature_names=["anxious", "cardiovascular", "compare_one_year", "day_to_day",
                                         "depressed", "emotional", "fatigued", "general_health",
                                         "mental_health", "physical_activities", "physical_health",
                                         "risk", "sleep_trouble", "social_activities", "stressed"],
                          drop_first=True),
    ])

mypipe = MyHealthDataset(os.environ['SLEEPHEALTH_MYHEALTH_PATH'], pipeline=pipeline)
df_piped = mypipe.processed_dataframe()
print("Shape:", df_piped.shape)
df_piped.head()

# %% pycharm={"name": "#%%\n"}
