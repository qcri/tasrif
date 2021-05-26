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
from tasrif.data_readers.my_heart_counts import DayOneSurveyDataset


# %%
dos_file_path = os.environ['MYHEARTCOUNTS_DAYONESURVEY_PATH']

# %%
dcs = DayOneSurveyDataset(dos_file_path, drop_device_na=True, drop_labwork_na=True)

# %%
dcs.raw_dataframe()

# %%
dcs.processed_dataframe()

# %%
DayOneSurveyDataset.device_mapping

# %%
dcs.participant_count()

# %%
