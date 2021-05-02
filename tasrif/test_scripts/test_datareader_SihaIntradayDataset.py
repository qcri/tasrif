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
folder='/home/ummar/Downloads/Data/'

# %%
ds = SihaSleepIntradayDataset(folder=folder)
df0 = ds.processed_dataframe()

# %%
df0

# %%
ds = SihaSleepDataset(folder='/home/ummar/Downloads/Data/profast')

df1 = ds.processed_dataframe()

# %%
df1

# %%
ds = SihaStepsIntradayDataset(folder='/home/ummar/Downloads/Data/profast')

df2 = ds.processed_dataframe()

# %%
df2

# %%
ds = SihaCaloriesIntradayDataset(folder='/home/ummar/Downloads/Data/profast')
df3 = ds.processed_dataframe()
df3

# %%
ds = SihaDistanceIntradayDataset(folder='/home/ummar/Downloads/Data/profast')
df4 = ds.processed_dataframe()
df4

# %%
ds = SihaHeartRateIntradayDataset(folder='/home/ummar/Downloads/Data/profast')
df5 = ds.processed_dataframe()
df5

# %%
ds = SihaVeryActiveMinutesDataset(folder='/home/ummar/Downloads/Data/profast')
df6 = ds.processed_dataframe()
df6

# %%
ds = SihaModeratelyActiveMinutesDataset(folder='/home/ummar/Downloads/Data/profast')
df7 = ds.processed_dataframe()
df7

# %%
ds = SihaLightlyActiveMinutesDataset(folder='/home/ummar/Downloads/Data/profast')
df8 = ds.processed_dataframe()
df8

# %%
ds = SihaSedentaryMinutesDataset(folder='/home/ummar/Downloads/Data/profast')
df9 = ds.processed_dataframe()

# %%
df9

# %%
ds = SihaTimeInHeartRateZonesDataset(folder='/home/ummar/Downloads/Data/profast')
df10 = ds.processed_dataframe()

# %%
df10

# %%
ds = SihaStepsDataset(folder='/home/ummar/Downloads/Data/profast')
df11 = ds.processed_dataframe()

# %%
df11

# %%
ds = SihaCaloriesDataset(folder='/home/ummar/Downloads/Data/profast')
df12 = ds.processed_dataframe()

# %%
df12

# %%
ds = SihaDistanceDataset(folder='/home/ummar/Downloads/Data/profast')
df13 = ds.processed_dataframe()

# %%
df13

# %%
ds = SihaCgmDataset(folder='/home/ummar/Downloads/Data/profast')
df13 = ds.processed_dataframe()

# %%
df13

# %%
ds = SihaEmrDataset(folder='/home/ummar/Downloads/Data/profast')
df14 = ds.processed_dataframe()

# %%
df14

# %%
ds = SihaSleepIntradayDataset(folder=folder)
df15 = ds.processed_dataframe()

# %%
