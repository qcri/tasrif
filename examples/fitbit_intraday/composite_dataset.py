import os

from tasrif.data_readers.fitbit_intraday_dataset import FitbitIntradayDataset
from tasrif.processing_pipeline import ComposeOperator, SequenceOperator
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConvertToDatetimeOperator,
    JsonNormalizeOperator,
    MergeOperator,
    SetIndexOperator,
)

fitbit_intraday_data_folder = os.environ["FITBIT_INTRADAY_PATH"]

composite_pipeline = SequenceOperator(
    [
        ComposeOperator(
            [
                FitbitIntradayDataset(
                    fitbit_intraday_data_folder,
                    table_name="Very_Active_Minutes",
                    num_files=5,
                ),
                FitbitIntradayDataset(
                    fitbit_intraday_data_folder,
                    table_name="Lightly_Active_Minutes",
                    num_files=5,
                ),
                FitbitIntradayDataset(
                    fitbit_intraday_data_folder,
                    table_name="Moderately_Active_Minutes",
                    num_files=5,
                ),
                FitbitIntradayDataset(
                    fitbit_intraday_data_folder,
                    table_name="Sedentary_Minutes",
                    num_files=5,
                ),
            ]
        ),
        JsonNormalizeOperator(),
        ConvertToDatetimeOperator(
            feature_names=["dateTime"], infer_datetime_format=True
        ),
        SetIndexOperator("dateTime"),
        AsTypeOperator({"value": "int32"}),
        MergeOperator(on="dateTime", how="outer"),
    ]
)

df = composite_pipeline.process()

print(df)

import yaml

import tasrif.yaml_parser as yaml_parser

# This is done because this file is executed within a unit test from a different directory
# The relative path would not work in that case.
# __file__ is not defined in iPython interactive shell
try:
    yaml_config_path = os.path.join(
        os.path.dirname(__file__), "yaml_config/composite_dataset.yaml"
    )
except:
    yaml_config_path = "yaml_config/composite_dataset.yaml"

with open(yaml_config_path, "r") as stream:
    try:
        #         print(json.dumps(yaml.safe_load(stream), indent=4, sort_keys=True))
        p = yaml_parser.from_yaml(stream)
    except yaml.YAMLError as exc:
        print(exc)

df = p.process()

print(df)
