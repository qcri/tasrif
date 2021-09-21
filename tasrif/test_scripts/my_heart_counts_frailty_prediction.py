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
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline import SequenceOperator, MapProcessingOperator, ReduceProcessingOperator
from tasrif.processing_pipeline.pandas import DropNAOperator, ConvertToDatetimeOperator, DropFeaturesOperator, \
                                              SetIndexOperator, PivotResetColumnsOperator, ConcatOperator, \
                                              MergeOperator, AsTypeOperator, JsonNormalizeOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator, AggregateOperator, FilterOperator, \
                                              ReadNestedCsvOperator, IterateJsonOperator, FlattenOperator
import warnings
warnings.filterwarnings("ignore")

# %%
# First, we extract six minute walk activity step count for each participant. Note that
# participants are idenitified by a unique healthCode and may have participated in
# multiple six minute walk activities.
smwa_file_path = os.environ['MYHEARTCOUNTS_SIXMINUTEWALKACTIVITY_PATH']
json_folder_path = os.environ['MYHEARTCOUNTS_SIXMINUTEWALKACTIVITY_JSON_FOLDER_PATH']

# To do this, we create a custom operator to emit some data from the SMWA rows and json files.
class EmitHealthCodeSMWAStepsOperator(MapProcessingOperator):
    def _processing_function(self, generator):
        data = []

        for row, smwa_data in generator:
            if smwa_data is None:
                continue
            healthCode = row.healthCode
            smwaSteps = smwa_data.iloc[-1]['numberOfSteps']
            data.append([healthCode, smwaSteps])

        return pd.DataFrame(data, columns=['healthCode', 'smwaSteps'])

json_pipeline = SequenceOperator([
    JsonNormalizeOperator()
])

# Use the custom operator along with some built-in operators to get the data we want.
smwa_pipeline = SequenceOperator([
    MyHeartCountsDataset(smwa_file_path),
    CreateFeatureOperator(
        feature_name='file_name',
        # The json filename has an extra '.0' appended to it.
        feature_creator=lambda df: str(df['pedometer_fitness.walk.items'])[:-2]),
    IterateJsonOperator(
        folder_path=json_folder_path,
        field='file_name',
        pipeline=json_pipeline),
    EmitHealthCodeSMWAStepsOperator(),
    AggregateOperator(
        groupby_feature_names=["healthCode"],
        aggregation_definition={'smwaSteps': 'max'}
    )
])

# We now have the highest recorded steps taken in a six minute walk activity
# for each participant.
participant_smwa_steps = smwa_pipeline.process()[0]

# %%
# Next, we extract the highest daily step count for each participant.
hkd_file_path = os.environ['MYHEARTCOUNTS_HEALTHKITDATA_PATH']
csv_folder_path = os.environ['MYHEARTCOUNTS_HEALTHKITDATA_CSV_FOLDER_PATH']

# Before that, we need a custom operator that enhances HealthKitData csv
# dataframes with the healthCode from their corresponding rows.
class AppendHealthCodeOperator(MapProcessingOperator):
    def _processing_function(self, generator):
        data = []

        for row, hkd_data in generator:
            if hkd_data is None:
                continue
            hkd_data['healthCode'] = hkd_data.apply(lambda df: row.healthCode, axis=1)
            data.append(hkd_data)

        return data

# Now, create a pipeline that filters the HealthKitData csv dataframes to return
# daily steps counts from a single source (in this case, an Apple phone).
csv_pipeline = SequenceOperator([
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

# We then use the custom operator from above to attach healthCodes to the csv data.
# Then, we aggregate all the daily steps taken by each user, and take the highest.
hkd_pipeline = SequenceOperator([
            MyHeartCountsDataset(hkd_file_path),
            CreateFeatureOperator(
                feature_name='file_name',
                feature_creator=lambda df: str(df['data.csv'])),
            ReadNestedCsvOperator(
                folder_path=csv_folder_path,
                field='file_name',
                pipeline=csv_pipeline),
            AppendHealthCodeOperator(),
            FlattenOperator(),
            ConcatOperator(),
            AggregateOperator(
                groupby_feature_names=["healthCode"],
                aggregation_definition={'HKQuantityTypeIdentifierStepCount': 'max'})
])

participant_most_steps_in_a_day = hkd_pipeline.process()[0]

# %%
# For each participant, merge his six minute walk activity steps with his most steps taken in a day.
participant_smwa_steps_vs_most_steps_in_a_day = MergeOperator(on='healthCode').process(
    participant_smwa_steps,
    participant_most_steps_in_a_day
)

# %%
# We finally have a dataframe that tells us, for each participant:
# - The number of steps taken in the six minute walk activity
# - The most steps taken in a day
participant_smwa_steps_vs_most_steps_in_a_day

# %%
import matplotlib.pyplot as plt

# Prune outliers with max steps > 20,000
data = participant_smwa_steps_vs_most_steps_in_a_day
data = data[data['HKQuantityTypeIdentifierStepCount_max'] < 20000]

plt.scatter(data['HKQuantityTypeIdentifierStepCount_max'], data['smwaSteps_max'], alpha=0.5)
plt.xlabel("Most steps taken in a day")
plt.ylabel("Six Minute Walk Activity steps")

# The plot shows that there seems to be no significant correlation between
# the most steps taken in a day vs the steps walked in a six minute walk activity
# for a participant.
plt.show()
