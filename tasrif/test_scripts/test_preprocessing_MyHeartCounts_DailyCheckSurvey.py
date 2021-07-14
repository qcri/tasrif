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

# %%
import os
from tasrif.data_readers.my_heart_counts import DailyCheckSurveyDataset

dcs_file_path = os.environ['MYHEARTCOUNTS_DAILYCHECKSURVEY_PATH']
dcs = DailyCheckSurveyDataset(dcs_file_path)

dcs.process()[0]
