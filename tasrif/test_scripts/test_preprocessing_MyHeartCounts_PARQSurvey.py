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
# # MyHeartCounts - Physical Activity Readiness Questionnaire

# %%
from tasrif.data_readers.my_heart_counts import PARQSurveyDataset


# %%
parq = PARQSurveyDataset(mhc_folder='E:\\Development\\siha')

# %%
parq.raw_dataframe()

# %%
parq.processed_dataframe()

# %%
parq.participant_count()
