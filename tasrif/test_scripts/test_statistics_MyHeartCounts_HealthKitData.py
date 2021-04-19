# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: env
#     language: python
#     name: env
# ---

# + tags=[]
import pandas as pd

from tasrif.data_readers.my_heart_counts import HealthKitDataDataset
from tasrif.processing_pipeline.custom import StatisticsOperator

hkd = HealthKitDataDataset(mhc_folder='/home/fabubaker/qcri/tasrif-project/MHC_subset_with_csv')
generator = hkd.processed_df[0]

dfs = []
for row, csv_df in generator:
    csv_df['healthCode'] = row.healthCode
    dfs.append(csv_df)

final_df = pd.concat(dfs)
sop = StatisticsOperator(participant_identifier='healthCode', date_feature_name='Date')
stats_df = sop.process(final_df)
stats_df[0]
