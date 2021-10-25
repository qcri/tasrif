"""
Sleep Quality Checker Dataset details can be found online at
``https://www.synapse.org/#!Synapse:syn18492837/wiki/593719``.

Some important stats:
    - This dataset contains unique data for 4,566 participants.
    - The default pipeline groups multiple entries for different`participantId` into one row per participant
      and multiple column with statistics for the sleep quality score (`sq_score`) of each participant.

"""

import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.sleep_health import SleepHealthDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SortOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator, AggregateOperator

sleephealth_path = os.environ['SLEEPHEALTH']

pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "sleepqualitychecker"),
    SortOperator(by=["participantId", "timestamp"]),
    AggregateOperator(
        groupby_feature_names="participantId",
        aggregation_definition={
            "sq_score": [
                "count",
                "mean",
                "std",
                "min",
                "max",
                "first",
                "last",
            ],
            "timestamp": ["first", "last"],
        },
    ),
    ConvertToDatetimeOperator(
        feature_names=["timestamp_last", "timestamp_first"],
        format="%Y-%m-%dT%H:%M:%S%z",
        utc=True,
    ),
    CreateFeatureOperator(
        feature_name="delta_first_last_timestamp",
        feature_creator=lambda row: row["timestamp_last"] - row[
            "timestamp_first"],
    ),
])

df = pipeline.process()

print(df)
