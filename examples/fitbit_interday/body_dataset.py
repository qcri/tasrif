import os

from tasrif.data_readers.fitbit_interday_dataset import FitbitInterdayDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    SetIndexOperator,
)

interday_folder_path = os.environ.get("FITBIT_INTERDAY_PATH", "/mnt/data/fitbit-data/")

pipeline = SequenceOperator(
    [
        FitbitInterdayDataset(interday_folder_path, table_name="Body"),
        ConvertToDatetimeOperator(feature_names=["Date"], infer_datetime_format=True),
        SetIndexOperator("Date"),
    ]
)

df = pipeline.process()

print(df)

import yaml

import tasrif.yaml_parser as yaml_parser

# This is done because this file is executed within a unit test from a different directory
# The relative path would not work in that case.
# __file__ is not defined in iPython interactive shell
try:
    yaml_config_path = os.path.join(
        os.path.dirname(__file__), "yaml_config/body_dataset.yaml"
    )
except:
    yaml_config_path = "yaml_config/body_dataset.yaml"

with open(yaml_config_path, "r") as stream:
    try:
        #         print(json.dumps(yaml.safe_load(stream), indent=4, sort_keys=True))
        p = yaml_parser.from_yaml(stream)
    except yaml.YAMLError as exc:
        print(exc)

df = p.process()

print(df)
