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
from tasrif.processing_pipeline.custom import AggregateOperator, AddDurationOperator
# %load_ext autoreload
# %autoreload 2

from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SortOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator

from tasrif.data_readers.sleep_health import SleepQualityCheckerDataset
import pandas as pd

# %%
# Full SleepQualityCheckerDataset
sqc = SleepQualityCheckerDataset(os.environ['SLEEPHEALTH_SLEEPQUALITYCHECKER_PATH'], pipeline=None)
df = sqc.raw_df.copy()
print("Shape:", df.shape)
df.head()

# %% pycharm={"name": "#%%\n"}
sqc.participant_count()

# %% pycharm={"name": "#%%\n"}

df0 = pd.DataFrame([['Doha', 25, 30], ['Doha', 17, 50], ['Dubai', 20, 40], ['Dubai', 21, 42]],
                    columns=['city', 'min_temp', 'max_temp'])

operator = AggregateOperator(groupby_feature_names ="city",
                             aggregation_definition= {"min_temp": ["mean", "std"]})
df0 = operator.process(df0)
df0

# %% pycharm={"name": "#%%\n"}
# Full SleepQualityCheckerDataset
pipeline = SequenceOperator([
    SortOperator(by=["participantId", "timestamp"]),
    AggregateOperator(groupby_feature_names="participantId",
                      aggregation_definition= {
                          "sq_score": ["count", "mean", "std", "min", "max", "first", "last"],
                          "timestamp": ["first", "last"]
                      }),
    ConvertToDatetimeOperator(feature_names=["timestamp_last", "timestamp_first"], format="%Y-%m-%dT%H:%M:%S%z", utc=True),
    CreateFeatureOperator(feature_name="delta_first_last_timestamp",
                          feature_creator=lambda row: row['timestamp_last'] - row['timestamp_first'])
    ])

sqc = SleepQualityCheckerDataset(os.environ['SLEEPHEALTH_SLEEPQUALITYCHECKER_PATH'], pipeline=pipeline)
df = sqc.processed_dataframe()
print("Shape:", df.shape)
df.head()


# %% pycharm={"name": "#%%\n"}
sqc.participant_count()

# %% pycharm={"name": "#%%\n"}
