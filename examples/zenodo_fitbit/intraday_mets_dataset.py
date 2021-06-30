"""Example to represent the intraday METs related CSV files of the fitbit dataset published on Zenodo"""
import os

from tasrif.processing_pipeline import (
    ProcessingPipeline,
    ComposeOperator,
    NoopOperator,
)
from tasrif.processing_pipeline.custom import (
    AggregateOperator,
    ResampleOperator,
)
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    GroupbyOperator,
    ResetIndexOperator,
    SetIndexOperator,
)

from tasrif.data_readers.zenodo_fitbit_dataset import ZenodoFitbitDataset

zenodo_folder_path = os.environ['ZENODO_FITBIT_PATH']

HOURLY_AGGREGATION_DEFINITION = {"METs": "sum"}

TOTAL_AGGREGATION_DEFINITION = {
    "METs": ["mean", "std"],
}

pipeline = ProcessingPipeline([
    ZenodoFitbitDataset(zenodo_folder_path, table_name="IntradayMETs"),
    ConvertToDatetimeOperator(feature_names=["ActivityMinute"], format="%m/%d/%Y %I:%M:%S %p"),
    SetIndexOperator(["ActivityMinute"]),
    GroupbyOperator(by="Id"),
    ResampleOperator(
        rule="H",
        aggregation_definition=HOURLY_AGGREGATION_DEFINITION),
    ResetIndexOperator(),
    ComposeOperator([
        NoopOperator(),
        AggregateOperator(
            groupby_feature_names=["Id"],
            aggregation_definition=TOTAL_AGGREGATION_DEFINITION,
        ),
    ]),
])


df = pipeline.process()

print(df)