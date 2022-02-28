"""Example that represents the activity related CSV files of the fitbit dataset published on Zenodo
"""
import os

import pandas as pd

from tasrif.data_readers.zenodo_fitbit_dataset import ZenodoFitbitDataset
from tasrif.processing_pipeline import ComposeOperator, NoopOperator, SequenceOperator
from tasrif.processing_pipeline.custom import AggregateOperator, CreateFeatureOperator
from tasrif.processing_pipeline.pandas import DropFeaturesOperator, DropNAOperator

zenodo_folder_path = os.environ.get("ZENODOFITBIT_PATH")

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

pipeline = SequenceOperator(
    [
        ZenodoFitbitDataset(zenodo_folder_path, table_name="Activity"),
        DropNAOperator(),
        CreateFeatureOperator(
            feature_name="ActiveMinutes",
            feature_creator=lambda df: df["VeryActiveMinutes"]
            + df["FairlyActiveMinutes"]
            + df["LightlyActiveMinutes"],
        ),
        CreateFeatureOperator(
            feature_name="Date",
            feature_creator=lambda df: pd.to_datetime(df["ActivityDate"]),
        ),
        DropFeaturesOperator(feature_names=DROP_FEATURES),
        ComposeOperator(
            [
                NoopOperator(),
                AggregateOperator(
                    groupby_feature_names="Id",
                    aggregation_definition=AGGREGATION_DEFINITION,
                ),
            ]
        ),
    ]
)

df = pipeline.process()

print(df)
