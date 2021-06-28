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
import pandas as pd
from tasrif.data_readers.my_heart_counts import SixMinuteWalkActivityDataset, HealthKitDataDataset
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.pandas import DropNAOperator, ConvertToDatetimeOperator, DropFeaturesOperator, \
                                              SetIndexOperator, PivotResetColumnsOperator, ConcatOperator, \
                                              MergeOperator, AsTypeOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator, AggregateOperator, FilterOperator
import warnings
warnings.filterwarnings("ignore")

# %%
# Extract SixMinuteWalkActivity step count for each participant.
smwa_file_path = os.environ['MYHEARTCOUNTS_SIXMINUTEWALKACTIVITY_PATH']
json_folder_path = os.environ['MYHEARTCOUNTS_SIXMINUTEWALKACTIVITY_JSON_FOLDER_PATH']

smwa = SixMinuteWalkActivityDataset(smwa_file_path, json_folder_path)
participant_smwa_steps = { 'healthCode': [], 'smwaSteps': [] }

for row, smwa_data in smwa.processed_dataframe()[0]:
    if smwa_data is None:
        continue
    participant_smwa_steps['healthCode'].append(row.healthCode)
    participant_smwa_steps['smwaSteps'].append(smwa_data.iloc[-1]['numberOfSteps'])

participant_smwa_steps = pd.DataFrame.from_dict(participant_smwa_steps)

# Take highest recorded steps for each healthCode across multiple six minute walk activites.
agg_operator = AggregateOperator(groupby_feature_names=["healthCode"], aggregation_definition={'smwaSteps': 'max'})
participant_smwa_steps = agg_operator.process(participant_smwa_steps)[0]

# %%
# Extract the highest daily step count for each participant.
hkd_file_path = os.environ['MYHEARTCOUNTS_HEALTHKITDATA_PATH']
csv_folder_path = os.environ['MYHEARTCOUNTS_HEALTHKITDATA_CSV_FOLDER_PATH']

# Modify the CSV pipeline to filter step counts from a singular source.
HealthKitDataDataset.Defaults.CSV_PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(
                feature_names=["endTime"],
                errors='coerce'),
            DropNAOperator(),
            CreateFeatureOperator(
                feature_name='Date',
                feature_creator=lambda df: df['endTime'].date()),
            DropFeaturesOperator(drop_features=['startTime', 'endTime']),
            FilterOperator(
                participant_id_column=None,
                ts_column="Date",
                epoch_filter=lambda df: (df['type'] == 'HKQuantityTypeIdentifierStepCount') &
                (df['sourceIdentifier'] == 'com.apple.health') & (df['source'] == 'phone')
            ),
            AsTypeOperator({'value': 'int32'}),
            AggregateOperator(
                groupby_feature_names=["Date", "type"],
                aggregation_definition={'value': 'sum'}),
            SetIndexOperator('Date'),
            PivotResetColumnsOperator(level=1, columns='type')
        ])

# Append healthCode to each processed dataframe
hkd = HealthKitDataDataset(hkd_file_path, csv_folder_path)
participant_daily_steps = []
for row, hkd_data in hkd.processed_dataframe()[0]:
    if hkd_data is None:
        continue
    hkd_data['healthCode'] = hkd_data.apply(lambda df: row.healthCode, axis=1)
    participant_daily_steps.append(hkd_data)

# Grab the max steps taken by each participant in a day.
pipeline = ProcessingPipeline([
    ConcatOperator(),
    AggregateOperator(
        groupby_feature_names=["healthCode"],
        aggregation_definition={'HKQuantityTypeIdentifierStepCount': 'max'})
])
participant_max_steps_in_a_day = pipeline.process(*participant_daily_steps)[0]

# Merge SixMinuteWalkActivity steps with max steps taken in a day.
merge_operator = MergeOperator(on='healthCode')
participant_smwa_steps_vs_max_steps_in_a_day = merge_operator.process(
    participant_smwa_steps,
    participant_max_steps_in_a_day
)

# %%
participant_smwa_steps_vs_max_steps_in_a_day

# %%
import matplotlib.pyplot as plt

# Prune outliers with max steps > 20,000
data = participant_smwa_steps_vs_max_steps_in_a_day
data = data[data['HKQuantityTypeIdentifierStepCount_max'] < 20000]

plt.scatter(data['HKQuantityTypeIdentifierStepCount_max'], data['smwaSteps_max'], alpha=0.5)
plt.xlabel("Maximum steps taken in a day")
plt.ylabel("Six Minute Walk Activity steps")

# The plot shows that there seems to be no significant correlation between
# the maximum steps taken in a day vs the steps walked in a Six Minute Walk Activity
# for a participant.
plt.show()
