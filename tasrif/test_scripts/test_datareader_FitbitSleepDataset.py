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
from tasrif.data_readers.fitbit_intraday_dataset import FitbitSleepDataset

# %%
sds = FitbitSleepDataset(folder='/home/ummar/Downloads/Data/VolunteerFitbitData-1March21/FahimDalvi/')

# %%
sds.raw_dataframe()

# %%
sds.processed_dataframe()
