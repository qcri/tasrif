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
from tasrif.data_readers.siha_dataset import \
    SihaSleepDataset, \
    SihaStepsIntradayDataset, \
    SihaCaloriesIntradayDataset, \
    SihaDistanceIntradayDataset, \
    SihaHeartRateIntradayDataset, \
    SihaVeryActiveMinutesDataset, \
    SihaLightlyActiveMinutesDataset, \
    SihaSedentaryMinutesDataset, \
    SihaModeratelyActiveMinutesDataset, \
    SihaTimeInHeartRateZonesDataset, \
    SihaStepsDataset, \
    SihaCaloriesDataset, \
    SihaDistanceDataset, \
    SihaCgmDataset, \
    SihaEmrDataset, \
    SihaSleepIntradayDataset

# %%
ds = SihaSleepIntradayDataset(os.environ['SIHA_PATH'])
df0 = ds.processed_dataframe()

# %%
df0

# %%
ds = SihaSleepDataset(os.environ['SIHA_PATH'])

df1 = ds.processed_dataframe()

# %%
df1

# %%
ds = SihaStepsIntradayDataset(os.environ['SIHA_PATH'])

df2 = ds.processed_dataframe()

# %%
df2

# %%
ds = SihaCaloriesIntradayDataset(os.environ['SIHA_PATH'])
df3 = ds.processed_dataframe()
df3

# %%
ds = SihaDistanceIntradayDataset(os.environ['SIHA_PATH'])
df4 = ds.processed_dataframe()
df4

# %%
ds = SihaHeartRateIntradayDataset(os.environ['SIHA_PATH'])
df5 = ds.processed_dataframe()
df5

# %%
ds = SihaVeryActiveMinutesDataset(os.environ['SIHA_PATH'])
df6 = ds.processed_dataframe()
df6

# %%
ds = SihaModeratelyActiveMinutesDataset(os.environ['SIHA_PATH'])
df7 = ds.processed_dataframe()
df7

# %%
ds = SihaLightlyActiveMinutesDataset(os.environ['SIHA_PATH'])
df8 = ds.processed_dataframe()
df8

# %%
ds = SihaSedentaryMinutesDataset(os.environ['SIHA_PATH'])
df9 = ds.processed_dataframe()

# %%
df9

# %%
ds = SihaTimeInHeartRateZonesDataset(os.environ['SIHA_PATH'])
df10 = ds.processed_dataframe()

# %%
df10

# %%
ds = SihaStepsDataset(os.environ['SIHA_PATH'])
df11 = ds.processed_dataframe()

# %%
df11

# %%
ds = SihaCaloriesDataset(os.environ['SIHA_PATH'])
df12 = ds.processed_dataframe()

# %%
df12

# %%
ds = SihaDistanceDataset(os.environ['SIHA_PATH'])
df13 = ds.processed_dataframe()

# %%
df13

# %%
ds = SihaCgmDataset(os.environ['SIHA_PATH'])
df13 = ds.processed_dataframe()

# %%
df13

# %%
ds = SihaEmrDataset(os.environ['SIHA_PATH'])
df14 = ds.processed_dataframe()

# %%
df14

# %%
ds = SihaSleepIntradayDataset(os.environ['SIHA_PATH'])
df15 = ds.processed_dataframe()

# %%
