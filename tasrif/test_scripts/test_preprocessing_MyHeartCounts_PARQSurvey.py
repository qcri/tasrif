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
# # MyHeartCounts - Physical Activity Readiness Questionnaire

# %%
import os
from tasrif.data_readers.my_heart_counts import PARQSurveyDataset

# %%
parq_file_path = os.environ['MYHEARTCOUNTS_PARQSURVEY_PATH']

# %%
parq = PARQSurveyDataset(parq_file_path)
# %%
parq.process()[0]
