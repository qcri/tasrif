"""Class that represents the intraday calories related CSV files of the fitbit dataset published on Zenodo"""
import os

from tasrif.processing_pipeline import (
    SequenceOperator,
    ComposeOperator,
    NoopOperator,
)
from tasrif.processing_pipeline.custom import (
    AggregateOperator,
    ResampleOperator,
)
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    SetIndexOperator,
    GroupbyOperator,
    ResetIndexOperator,
)

from tasrif.data_readers.zenodo_fitbit_dataset import ZenodoFitbitDataset

zenodo_folder_path = os.environ.get('ZENODOFITBIT_PATH')

HOURLY_AGGREGATION_DEFINITION = {"Calories": "sum"}

TOTAL_AGGREGATION_DEFINITION = {
    "Calories": ["mean", "std"],
}

pipeline = SequenceOperator([
    ZenodoFitbitDataset(zenodo_folder_path, table_name="IntradayCalories"),
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

import tasrif.yaml_parser as yaml_parser
import yaml

with open("yaml_config/intraday_calories_dataset.yaml", "r") as stream:
    try:
#         print(json.dumps(yaml.safe_load(stream), indent=4, sort_keys=True))
        p = yaml_parser.from_yaml(stream)
    except yaml.YAMLError as exc:
        print(exc)

df = p.process()

print(df)


