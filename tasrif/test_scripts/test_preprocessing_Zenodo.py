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

# %% [markdown]
# # Zenodo - preprocessing

# %% [markdown]
# # Preprocessing

# %% tags=[]
import os
import pandas as pd
import numpy as np
import pathlib
import datetime
from tasrif.data_readers.zenodo_fitbit_dataset import (
    ZenodoFitbitActivityDataset,
    ZenodoFitbitWeightDataset,
    ZenodoFitbitSleepDataset,
    ZenodoCompositeFitbitDataset,
    ZenodoFitbitIntradayStepsDataset)

nan = np.nan
zenodo_folder = os.environ['ZENODOFITBIT_PATH']

zfd = ZenodoFitbitActivityDataset(zenodo_folder=zenodo_folder)
df = zfd.processed_dataframe()
adf = zfd.grouped_dataframe()
pdfs = zfd.participant_dataframes()

zwd = ZenodoFitbitWeightDataset(zenodo_folder=zenodo_folder)
zwd.raw_dataframe()
zwd.processed_dataframe()
wdf = zwd.grouped_dataframe()

zsd = ZenodoFitbitSleepDataset(zenodo_folder=zenodo_folder)
df = zsd.processed_dataframe()
sdf = zsd.grouped_dataframe()
#[pd.DataFrame(y) for x, y in df.groupby('logId', as_index=False)]

steps_df_init = ZenodoFitbitIntradayStepsDataset(zenodo_folder=zenodo_folder)
steps_df = steps_df_init.processed_dataframe()
steps_df_grouped = steps_df_init.grouped_dataframe()

# %%
adf

# %%
zfd.participant_count()

# %%
sdf

# %%
zfd.processed_dataframe()

# %%
df = df.dropna(axis=1)
df = df.drop(['sleep_episodes_count', 'Id'], axis=1)
df

# %%
cds = ZenodoCompositeFitbitDataset([zfd, zwd, zsd])
df = cds.grouped_dataframe()
df

# %%

# %%
from sihatk.dimensionality_reduction.dimensionality_reduction import identify_confounding_variables

# %%
identify_confounding_variables(df)

# %%
corr_df = df.corr()


# %%
corr_df

# %%
zsd

# %%
steps_df
