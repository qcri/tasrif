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
import os
import pandas as pd

qol_file_path = os.environ['MYHEARTCOUNTS_QUALITYOFLIFE_PATH']
df = pd.read_csv(qol_file_path)


def col_stats(df):
    print('Some important stats:')
    print('Shape:', df.shape)
    print('\t- This dataset contains unique data for ', len(df) ,'participants.')
    for col in df.columns:
        null_percentage = "{:.2f}".format(df[col].isnull().sum()/len(df)*100)
        print('\t - `', col, '` has', df[col].isnull().sum(), 'NAs (', df[col].count().sum(), '/', len(df), ') =',
              null_percentage, '%')

col_stats(df)

from tasrif.data_readers.my_heart_counts import QualityOfLifeSurveyDataset

qol = QualityOfLifeSurveyDataset(qol_file_path)

# %%
qol.raw_dataframe()

# %%
qol.processed_dataframe()

# %%
qol.participant_count()
