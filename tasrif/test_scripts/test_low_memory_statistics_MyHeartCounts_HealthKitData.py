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

sop = StatisticsOperator(participant_identifier='healthCode', date_feature_name='Date')

# Iterate through each dataframe and acculumate statistics in 'final_stats'.
final_stats = pd.DataFrame()
for row, csv_df in generator:
    csv_df['healthCode'] = row.healthCode
    csv_stats = sop.process(csv_df)[0]

    combined_stats = pd.concat([final_stats, csv_stats])
    final_stats = combined_stats.groupby(combined_stats.index).aggregate({
        'row_count': 'sum',
        'missing_data_count': 'sum',
        'duplicate_rows_count': 'sum',
        'participant_count': 'sum',
        'min_date': 'min',
        'max_date': 'max'
    })
    final_stats['duration'] = final_stats['max_date'] - final_stats['min_date']

final_stats
