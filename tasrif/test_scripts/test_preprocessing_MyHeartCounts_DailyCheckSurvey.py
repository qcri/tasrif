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

# %% [markdown]
# # MyHeartCounts - Daily Check Survey test script

# %% [markdown]
# # Using the DailyCheckSurveyDataset

# %%
import pandas as pd
from tasrif.data_readers.my_heart_counts import DailyCheckSurveyDataset


# %%
dcs_df = pd.read_csv('E:\\Development\\siha\\Daily Check Survey.csv')

# %%
dcs_df['activity1_option']

# %%
~dcs_df['activity1_option'].astype(bool)

# %%
dcs = DailyCheckSurveyDataset(mhc_folder='E:\\Development\\siha')

# %%
dcs.dcs_df[dcs.dcs_df['activity1_intensity'] == 4]

# %%
dcs

# %%
dcs.raw_dataframe()

# %%
dcs.processed_dataframe()

# %%
dcs.grouped_dataframe()

# %%
from tasrif.data_readers.my_heart_counts import DailyCheckSurveyDataset

# %%
dcs = DailyCheckSurveyDataset('E:\\Development\\siha', merge_activity_features=True)

# %%
dcs.raw_dataframe()

# %%
dcs.processed_dataframe()

# %%
dcs.grouped_dataframe()

# %%
dcs.participant_count()

