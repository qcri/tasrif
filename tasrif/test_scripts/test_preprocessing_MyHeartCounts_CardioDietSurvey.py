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
from tasrif.data_readers.my_heart_counts import CardioDietSurveyDataset


cds = CardioDietSurveyDataset(mhc_folder='E:\\Development\\siha')

print(cds.raw_dataframe())
print(cds.processed_dataframe())
print(cds.participant_count())
