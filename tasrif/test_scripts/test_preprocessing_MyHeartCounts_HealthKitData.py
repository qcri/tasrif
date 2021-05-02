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

from tasrif.data_readers.my_heart_counts import HealthKitDataDataset

hkd = HealthKitDataDataset(mhc_folder='/mnt/c/Development/projects/siha/MHC_subset_with_csv')
record, df = next(hkd.processed_df)

# %%
hkd.raw_df

# %%
record

# %%
df
