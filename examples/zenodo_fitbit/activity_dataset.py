"""Example that represents the activity related CSV files of the fitbit dataset published on Zenodo
"""
import os
import pandas as pd

from tasrif.processing_pipeline import (
    ProcessingPipeline,
    ComposeOperator,
    NoopOperator,
)
from tasrif.processing_pipeline.custom import (
    CreateFeatureOperator,
    AggregateOperator,
)
from tasrif.processing_pipeline.pandas import (
    DropNAOperator,
    DropFeaturesOperator,
)

from tasrif.data_readers.zenodo_fitbit_dataset import ZenodoFitbitDataset


zenodo_folder_path = os.environ['ZENODO_FITBIT_PATH']

DROP_FEATURES = [
    "TrackerDistance",
    "LoggedActivitiesDistance",
    "VeryActiveDistance",
    "ModeratelyActiveDistance",
    "SedentaryActiveDistance",
    "LightActiveDistance",
    "ActivityDate",
]

AGGREGATION_FUNCS = ["mean", "std"]

AGGREGATION_DEFINITION = {
    "TotalSteps": AGGREGATION_FUNCS,
    "TotalDistance": AGGREGATION_FUNCS,
    "SedentaryMinutes": AGGREGATION_FUNCS,
    "Calories": AGGREGATION_FUNCS,
    "ActiveMinutes": AGGREGATION_FUNCS,
}

pipeline = ProcessingPipeline([
    ZenodoFitbitDataset(zenodo_folder_path, table_name="Activity"),
    DropNAOperator(),
    CreateFeatureOperator(
        feature_name="ActiveMinutes",
        feature_creator=lambda df: df["VeryActiveMinutes"] + df[
            "FairlyActiveMinutes"] + df["LightlyActiveMinutes"],
    ),
    CreateFeatureOperator(
        feature_name="Date",
        feature_creator=lambda df: pd.to_datetime(df["ActivityDate"]),
    ),
    DropFeaturesOperator(drop_features=DROP_FEATURES),
    ComposeOperator([
        NoopOperator(),
        AggregateOperator(
            groupby_feature_names="Id",
            aggregation_definition=AGGREGATION_DEFINITION,
        ),
    ]),
])

df = pipeline.process()

print(df)
