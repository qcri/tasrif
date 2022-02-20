import os

from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.custom import AggregateOperator
from tasrif.processing_pipeline.pandas import DropFeaturesOperator

dcs_file_path = os.environ["MYHEARTCOUNTS_DAILYCHECKSURVEY_PATH"]

"""
Modifies the dataset by dropping unnecessary columns (features),
filling null activity intensity,
filling null activity time values,
and averages activity1 and activity2 if given the option.

Steps:
    - start with raw dataframe
    - for each paricipant (represented by healthCode column) in self.dcs_df
        - get the mean of activity1_intensity, activity1_time
        - get the standard deviation of activity1_intensity, activity1_time
        - repeat for activity2_option
"""
pipeline = SequenceOperator(
    [
        MyHeartCountsDataset(dcs_file_path),
        DropFeaturesOperator(["recordId"]),
        AggregateOperator(
            groupby_feature_names="healthCode",
            aggregation_definition={
                "activity1_intensity": ["mean", "std"],
                "activity1_time": ["mean", "std"],
                "activity2_intensity": ["mean", "std"],
                "activity2_time": ["mean", "std"],
                "sleep_time": ["mean", "std"],
            },
        ),
    ]
)

df = pipeline.process()

print(df)
