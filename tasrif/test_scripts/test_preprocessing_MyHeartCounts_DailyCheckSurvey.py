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
# # MyHeartCounts - Daily Check Survey test script

# %% [markdown]
# # Using the DailyCheckSurveyDataset

# %%
import os
import pandas as pd
from tasrif.data_readers.my_heart_counts import DailyCheckSurveyDataset


# %%
dcs_file_path = os.environ['MYHEARTCOUNTS_DAILYCHECKSURVEY_PATH']

# %%
dcs_df = pd.read_csv(dcs_file_path)

# %%
dcs_df['activity1_option']

# %%
~dcs_df['activity1_option'].astype(bool)

# %%
dcs = DailyCheckSurveyDataset(dcs_file_path)

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
dcs = DailyCheckSurveyDataset(dcs_file_path, merge_activity_features=True)

# %%
dcs.raw_dataframe()

# %%
dcs.processed_dataframe()

# %%
dcs.grouped_dataframe()

# %%
dcs.participant_count()

