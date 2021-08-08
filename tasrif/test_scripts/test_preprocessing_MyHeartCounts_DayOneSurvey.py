# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # MyHeartCounts - Day One Survey

# %%
import os
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset

# %%
dos_file_path = os.environ['MYHEARTCOUNTS_DAYONESURVEY_PATH']

# %%
dcs = MyHeartCountsDataset(dos_file_path)

# %%
dcs.process()[0]

# %%
MyHeartCountsDataset.day_one_survey_device_mapping
