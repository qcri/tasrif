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
import pandas as pd
import numpy as np
import pathlib
import datetime
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
ds = FitbitPhysicalActivityCaloriesDataset(folder='/home/ummar/Downloads/Data/VolunteerFitbitData-1March21/FahimDalvi/')

df1 = ds.processed_dataframe()

# %%
df1

# %%
ds = FitbitPhysicalActivityDistanceDataset(folder='/home/ummar/Downloads/Data/VolunteerFitbitData-1March21/FahimDalvi/')

df2 = ds.processed_dataframe()

# %%
df2

# %%
ds = FitbitPhysicalActivityHeartRateDataset(folder='/home/ummar/Downloads/Data/VolunteerFitbitData-1March21/FahimDalvi/')
df3 = ds.processed_dataframe()

# %%
ds = FitbitPhysicalActivityVeryActiveMinutesDataset(folder='/home/ummar/Downloads/Data/VolunteerFitbitData-1March21/FahimDalvi/')
df4 = ds.processed_dataframe()
df4

# %%
ds = FitbitPhysicalActivityLightlyActiveMinutesDataset(folder='/home/ummar/Downloads/Data/VolunteerFitbitData-1March21/FahimDalvi/')
df5 = ds.processed_dataframe()
df5

# %%
ds = FitbitPhysicalActivitySedentaryMinutesDataset(folder='/home/ummar/Downloads/Data/VolunteerFitbitData-1March21/FahimDalvi/')
df6 = ds.processed_dataframe()
df6

# %%
ds = FitbitPhysicalActivityModeratelyActiveMinutesDataset(folder='/home/ummar/Downloads/Data/VolunteerFitbitData-1March21/FahimDalvi/')
df7 = ds.processed_dataframe()
df7

# %%
ds = FitbitPhysicalActivityTimeInHeartRateZonesDataset(folder='/home/ummar/Downloads/Data/VolunteerFitbitData-1March21/FahimDalvi/')
df8 = ds.processed_dataframe()
df8

# %%
ds = FitbitPhysicalActivityStepsDataset(folder='/home/ummar/Downloads/Data/VolunteerFitbitData-1March21/FahimDalvi/')
df9 = ds.processed_dataframe()

# %%
df9

# %%
df1.merge(df2, how="outer", on="dateTime", suffixes=['_calories', '_distance'])

# %%
from tasrif.data_readers.fitbit_intraday_dataset import FitbitSleepDataset

# %%
ds = FitbitSleepDataset(folder='/home/ummar/Downloads/Data/VolunteerFitbitData-1March21/FahimDalvi/')

# %%
df10 = ds.processed_dataframe()

# %%
df10
