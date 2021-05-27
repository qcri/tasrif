# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import os
import pandas as pd
import numpy as np
import pathlib
import datetime
from tasrif.data_readers.withings_dataset import WithingsStepsIntradayDataset, WithingsSleepDataset, WithingsVerticalRadiusIntradayDataset, WithingsSleepStateIntradayDataset, WithingsQualityScoreIntradayDataset, WithingsLatLongIntradayDataset,\
WithingsAltitudeIntradayDataset, WithingsCaloriesIntradayDataset, WithingsDistanceIntradayDataset, WithingsHeartRateIntradayDataset,\
WithingsWeightDataset, WithingsHeightDataset, WithingsActivitiesDataset

# %%
ds = WithingsStepsIntradayDataset(folder=os.environ['WITHINGS_PATH'])
df_steps_raw = ds.raw_dataframe()
df_steps = ds.processed_dataframe()
df_steps

# %%
ds = WithingsQualityScoreIntradayDataset(os.environ['WITHINGS_PATH'])
df_qs_raw = ds.raw_dataframe()
df_qs = ds.processed_dataframe()
df_qs

# %%
ds = WithingsHeartRateIntradayDataset(os.environ['WITHINGS_PATH'])
df_hr_raw = ds.raw_dataframe()
df_hr = ds.processed_dataframe()
df_hr

# %%
ds = WithingsLatLongIntradayDataset(os.environ['WITHINGS_PATH'])
df_latlong_raw = ds.raw_dataframe()
df_latlong = ds.processed_dataframe()
df_latlong

# %%
ds = WithingsAltitudeIntradayDataset(os.environ['WITHINGS_PATH'])
df_altitude_raw = ds.raw_dataframe()
df_altitude = ds.processed_dataframe()
df_altitude

# %%
ds = WithingsWeightDataset(os.environ['WITHINGS_PATH'])
df_weight = ds.processed_dataframe()
df_weight

# %%
ds = WithingsHeightDataset(os.environ['WITHINGS_PATH'])
df_height = ds.processed_dataframe()
df_height

# %%
ds = WithingsSleepDataset(os.environ['WITHINGS_PATH'])
df_sleep = ds.processed_dataframe()
df_sleep

# %%
ds = WithingsActivitiesDataset(os.environ['WITHINGS_PATH'])
df_activities = ds.processed_dataframe()
df_activities
