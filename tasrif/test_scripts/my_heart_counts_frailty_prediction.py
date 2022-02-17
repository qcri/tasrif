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
from tasrif.processing_pipeline import (
    MapProcessingOperator,
    ReduceProcessingOperator,
    SequenceOperator,
)
from tasrif.processing_pipeline.custom import (
    AggregateOperator,
    CreateFeatureOperator,
    FilterOperator,
    FlattenOperator,
    IterateJsonOperator,
    ReadNestedCsvOperator,
)
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConcatOperator,
    ConvertToDatetimeOperator,
    DropFeaturesOperator,
    DropNAOperator,
    JsonNormalizeOperator,
    MergeOperator,
    PivotResetColumnsOperator,
    SetIndexOperator,
    SortOperator,
)

# %%
# First, we extract six minute walk activity step count for each participant. Note that
# participants are idenitified by a unique healthCode and may have participated in
# multiple six minute walk activities.
smwa_file_path = os.environ['MYHEARTCOUNTS']
json_folder_path = os.environ['MYHEARTCOUNTS'] + 'Six Minute Walk Activity/pedometer_fitness.walk.items/'
csv_folder_path = os.environ['MYHEARTCOUNTS'] + 'HealthKit Data/data.csv/'

smwa_pipeline = SequenceOperator([ConvertToDatetimeOperator(['startDate', 'endDate'], utc=True),
                                  DropNAOperator(),
                                  SortOperator(by='startDate')])

pipeline = SequenceOperator([
    MyHeartCountsDataset(path_name=smwa_file_path, table_name='sixminutewalkactivity',
                         nested_files_path=json_folder_path, participants=5,
                         nested_files_pipeline=smwa_pipeline),
    AggregateOperator(
        groupby_feature_names=["recordId"],
        aggregation_definition={'numberOfSteps': 'max'}
    )
])


# We now have the highest recorded steps taken in a six minute walk activity
# for each participant.
participant_smwa_steps = pipeline.process()[0]

# We then use the custom operator from above to attach healthCodes to the csv data.
# Then, we aggregate all the daily steps taken by each user, and take the highest.
hkd_pipeline = SequenceOperator([
            MyHeartCountsDataset(path_name=smwa_file_path,
                                 table_name='healthkitdata', 
                                 nested_files_path=csv_folder_path,
                                 participants=5),
])

participant_most_steps_in_a_day = hkd_pipeline.process()
participant_most_steps_in_a_day[0]
