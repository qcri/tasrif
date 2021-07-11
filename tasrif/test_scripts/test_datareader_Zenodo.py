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
from tasrif.data_readers.zenodo_fitbit_dataset import (
    ZenodoFitbitActivityDataset,
    ZenodoFitbitWeightDataset,
    ZenodoFitbitSleepDataset,
    ZenodoCompositeFitbitDataset)

nan = np.nan

zenodo_folder = os.environ['ZENODOFITBIT_PATH']

zfd = ZenodoFitbitActivityDataset(zenodo_folder=zenodo_folder)
df = zfd.processed_dataframe()
adf = zfd.grouped_dataframe()
pdfs = zfd.participant_dataframes()

zwd = ZenodoFitbitWeightDataset(zenodo_folder=zenodo_folder)
zwd.raw_dataframe()
df2 = zwd.processed_dataframe()
wdf = zwd.grouped_dataframe()

zsd = ZenodoFitbitSleepDataset(zenodo_folder=zenodo_folder)
df3 = zsd.processed_dataframe()
sdf = zsd.grouped_dataframe()
[pd.DataFrame(y) for x, y in df.groupby('logId', as_index=False)]



# %%
df2

# %%
zsd = ZenodoFitbitSleepDataset(zenodo_folder=zenodo_folder)
df = zsd.processed_dataframe()
sdf = zsd.grouped_dataframe()
#[pd.DataFrame(y) for x, y in df.groupby('logId', as_index=False)]

# %%
df

# %%
sdf

# %%
cds = ZenodoCompositeFitbitDataset([zfd, zwd, zsd])
df = cds.grouped_dataframe()


# %%
import pandas as pd

import numpy as np
import pathlib
import datetime
from tasrif.data_readers.zenodo_fitbit_dataset import (
    ZenodoFitbitActivityDataset,
    ZenodoFitbitWeightDataset,
    ZenodoFitbitSleepDataset,
    ZenodoCompositeFitbitDataset)

nan = np.nan

zfd = ZenodoFitbitActivityDataset(zenodo_folder=zenodo_folder)
df = zfd.processed_dataframe()
adf = zfd.grouped_dataframe()
pdfs = zfd.participant_dataframes()

# %%
from dataprep.eda import create_report

# %%
create_report(df)

# %%

import matplotlib.pyplot as plt

fig = plt.figure(figsize=(65, 100))
df['date'] = df['Date'].dt.strftime('%m-%d')
ax = df2[0].plot.bar(x="date-short", y="Id_count")
ax.set_xticklabels(labels=df2[0]['date-short'], rotation=70, rotation_mode="anchor", ha="right")
plt.tight_layout()

# %%
