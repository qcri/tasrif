"""Example to represent the body weight related CSV files of the fitbit dataset published on Zenodo"""
import os

from tasrif.processing_pipeline import (
    SequenceOperator,
    ComposeOperator,
    NoopOperator,
)

from tasrif.processing_pipeline.custom import AggregateOperator
from tasrif.processing_pipeline.pandas import DropFeaturesOperator
from tasrif.data_readers.zenodo_fitbit_dataset import ZenodoFitbitDataset

zenodo_folder_path = os.environ['ZENODO_FITBIT_PATH']


DROP_COLUMNS = ["Fat", "WeightPounds", "IsManualReport"]

AGGREGATION_FUNCS = ["mean", "std"]
AGGREGATION_DEFINITION = {
    "WeightKg": AGGREGATION_FUNCS,
    "BMI": AGGREGATION_FUNCS,
}

pipeline = SequenceOperator([
    ZenodoFitbitDataset(zenodo_folder_path, table_name="Weight"),
    DropFeaturesOperator(drop_features=DROP_COLUMNS),
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
