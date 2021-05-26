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
import os
from tasrif.data_readers.fitbit_intraday_dataset import \
    FitbitPhysicalActivityCaloriesDataset, \
    FitbitPhysicalActivityDistanceDataset, \
    FitbitPhysicalActivityHeartRateDataset,\
    FitbitPhysicalActivityVeryActiveMinutesDataset,\
    FitbitPhysicalActivityLightlyActiveMinutesDataset,\
    FitbitPhysicalActivitySedentaryMinutesDataset,\
    FitbitPhysicalActivityModeratelyActiveMinutesDataset,\
    FitbitPhysicalActivityTimeInHeartRateZonesDataset,\
    FitbitPhysicalActivityStepsDataset,\
    FitbitIntradayCompositeDataset

# %%
ds = FitbitPhysicalActivityCaloriesDataset(os.environ['FITBIT_INTRADAY_PATH'])

df1 = ds.processed_dataframe()

# %%
df1

# %%
ds = FitbitPhysicalActivityDistanceDataset(os.environ['FITBIT_INTRADAY_PATH'])

df2 = ds.processed_dataframe()

# %%
df2

# %%
ds = FitbitPhysicalActivityHeartRateDataset(os.environ['FITBIT_INTRADAY_PATH'])
df3 = ds.processed_dataframe()

# %%
ds = FitbitPhysicalActivityVeryActiveMinutesDataset(os.environ['FITBIT_INTRADAY_PATH'])
df4 = ds.processed_dataframe()
df4

# %%
ds = FitbitPhysicalActivityLightlyActiveMinutesDataset(os.environ['FITBIT_INTRADAY_PATH'])
df5 = ds.processed_dataframe()
df5

# %%
ds = FitbitPhysicalActivitySedentaryMinutesDataset(os.environ['FITBIT_INTRADAY_PATH'])
df6 = ds.processed_dataframe()
df6

# %%
ds = FitbitPhysicalActivityModeratelyActiveMinutesDataset(os.environ['FITBIT_INTRADAY_PATH'])
df7 = ds.processed_dataframe()
df7

# %%
ds = FitbitPhysicalActivityTimeInHeartRateZonesDataset(os.environ['FITBIT_INTRADAY_PATH'])
df8 = ds.processed_dataframe()
df8

# %%
ds = FitbitPhysicalActivityStepsDataset(os.environ['FITBIT_INTRADAY_PATH'])
df9 = ds.processed_dataframe()

# %%
df9

# %%
df1.merge(df2, how="outer", on="dateTime", suffixes=['_calories', '_distance'])

# %%
from tasrif.data_readers.fitbit_intraday_dataset import FitbitSleepDataset

# %%
ds = FitbitSleepDataset(os.environ['FITBIT_INTRADAY_PATH'])

# %%
df10 = ds.processed_dataframe()

# %%
df10
