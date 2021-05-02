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

df = pd.read_csv('E:\\Development\\siha\\Heart Age Survey.csv')


def col_stats(df):
    print('Some important stats:')
    print('\t- This dataset contains unique data for ', len(df) ,'participants.')
    for col in df.columns:
        null_percentage = "{:.2f}".format(df[col].isnull().sum()/len(df)*100)
        print('\t - `', col, '` has', df[col].isnull().sum(), 'NAs (', df[col].count().sum(), '/', len(df), ') =', 
              null_percentage, '%')
        
col_stats(df)

from tasrif.data_readers.my_heart_counts import HeartAgeSurveyDataset

has = HeartAgeSurveyDataset(mhc_folder='E:\\Development\\siha\\')

# %%
has.raw_dataframe()

# %%
has.processed_dataframe()

# %%
has.participant_count()
