# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: env
#     language: python
#     name: env
# ---

# %%
import os
from tasrif.data_readers.my_heart_counts import SixMinuteWalkActivityDataset

smwa_file_path = os.environ['MYHEARTCOUNTS_SIXMINUTEWALKACTIVITY_PATH']
json_folder_path = os.environ['MYHEARTCOUNTS_SIXMINUTEWALKACTIVITY_JSON_FOLDER_PATH']
smwa = SixMinuteWalkActivityDataset(smwa_file_path, json_folder_path)

smwa.raw_dataframe()
next(smwa.processed_dataframe()[0])
