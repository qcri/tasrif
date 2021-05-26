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
from tasrif.data_readers.fitbit_intraday_dataset import FitbitSleepDataset

# %%
sds = FitbitSleepDataset(os.environ['FITBIT_INTRADAY_PATH'])

# %%
sds.raw_dataframe()

# %%
sds.processed_dataframe()
