import os
import pandas as pd
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import DropFeaturesOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator, SetFeaturesValueOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset

dcs_file_path = os.environ['MYHEARTCOUNTS_DAILYCHECKSURVEY_PATH']

'''
Modifies the dataset by dropping unnecessary columns (features),
filling null activity intensity,
filling null activity time values,
and averages activity1 and activity2 if given the option.

Steps:
    - start with raw dataframe
    - drop self.drop_features features
    - set activity1_option null values to false if activity2_option is filled
    - set activity1_intensity to 4 if activity1_option and activity1_time are filled
    - set activity1_intensity and activity1_time to 0 if activity1_option is false
    - repeat the three steps above for activity2_option
    - drop activity1_option and activity2_option from
    - average the intensities if self.merge_activity_features is true, set it to column 'activity_intensity'
    - average the time if self.merge_activity_features is true, set it to column 'activity_time'
'''
pipeline = SequenceOperator([
    MyHeartCountsDataset(dcs_file_path),
    DropFeaturesOperator([
        "appVersion",
        "phoneInfo",
        "activity1_type",
        "activity2_type",
        "phone_on_user",
    ]),
    CreateFeatureOperator(
        feature_name="activity1_option",
        feature_creator=lambda df: bool(df["activity1_option"]),
    ),
    CreateFeatureOperator(
        feature_name="activity2_option",
        feature_creator=lambda df: bool(df["activity2_option"]),
    ),
    SetFeaturesValueOperator(
        selector=lambda df: df["activity1_option"].isnull()
        & df["activity2_option"].notnull(),
        features=["activity1_option"],
        value=False,
    ),
    SetFeaturesValueOperator(
        selector=lambda df: df["activity2_option"].isnull()
        & df["activity1_option"].notnull(),
        features=["activity2_option"],
        value=False,
    ),
    SetFeaturesValueOperator(
        selector=lambda df: pd.notnull(df["activity1_option"])
        & df["activity1_option"]
        & pd.notnull(df["activity1_time"])
        & pd.isnull(df["activity1_intensity"]),
        features=["activity1_intensity"],
        value=4,
    ),
    SetFeaturesValueOperator(
        selector=lambda df: pd.notnull(df["activity2_option"])
        & df["activity2_option"]
        & pd.notnull(df["activity2_time"])
        & pd.isnull(df["activity2_intensity"]),
        features=["activity2_intensity"],
        value=4,
    ),
    SetFeaturesValueOperator(
        selector=lambda df: pd.notnull(df["activity1_option"])
        & ~df["activity1_option"],
        features=["activity1_intensity", "activity1_time"],
        value=0,
    ),
    SetFeaturesValueOperator(
        selector=lambda df: pd.notnull(df["activity2_option"])
        & ~df["activity2_option"],
        features=["activity2_intensity", "activity2_time"],
        value=0,
    ),
    DropFeaturesOperator(["activity1_option", "activity2_option"]),
    CreateFeatureOperator(
        feature_name="activity_intensity",
        feature_creator=lambda df:
        (df["activity1_intensity"] + df["activity2_intensity"]) / 2,
    ),
    CreateFeatureOperator(
        feature_name="activity_time",
        feature_creator=lambda df:
        (df["activity1_time"] + df["activity2_time"]) / 2,
    ),
])

df = pipeline.process()

print(df)
