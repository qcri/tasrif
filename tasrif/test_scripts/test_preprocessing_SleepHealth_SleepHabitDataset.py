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
from tasrif.data_readers.sleep_health import SleepHabitDataset

from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import DropNAOperator
from tasrif.processing_pipeline.pandas import ReplaceOperator
from tasrif.processing_pipeline.custom import OneHotEncoderOperator


# %%
def col_stats(df):
    print('Some important stats:')
    print('\t- This dataset contains', len(df) ,' rows.')
    for col in df.columns:
        null_percentage = "{:.2f}".format(df[col].isnull().sum()/len(df)*100)
        print('\t - ``', col, '`` has', df[col].isnull().sum(), 'NAs (', df[col].count().sum(), '/', len(df), ') =',
              null_percentage, '%')



# %%
# Full AboutMeDataset
shd = SleepHabitDataset(os.environ['SLEEPHEALTH_SLEEPHABIT_PATH'], pipeline=None)

df_full = shd.processed_dataframe()
print("Full Shape:", df_full.shape)
col_stats(df_full)

# %%
shd = SleepHabitDataset(os.environ['SLEEPHEALTH_SLEEPHABIT_PATH'])

df_full = shd.processed_dataframe()
print("Full Shape:", df_full.shape)
col_stats(df_full)

# %%
# Checks if timestamp datatype is datetime64
df_full["timestamp"].dtype
