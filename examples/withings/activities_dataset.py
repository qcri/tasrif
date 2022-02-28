import os

from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.custom import JsonPivotOperator
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    SetIndexOperator,
)

withings_data_filename = os.environ["WITHINGS_PATH"] + "activities.csv"

pipeline = SequenceOperator(
    [
        WithingsDataset(withings_data_filename, table_name="Activities"),
        JsonPivotOperator(["Data", "GPS"]),
        ConvertToDatetimeOperator(
            feature_names=["from", "to"], infer_datetime_format=True, utc=True
        ),
        SetIndexOperator("from"),
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
        os.path.dirname(__file__), "yaml_config/activities_dataset.yaml"
    )
except:
    yaml_config_path = "yaml_config/activities_dataset.yaml"

with open(yaml_config_path, "r") as stream:
    try:
        #         print(json.dumps(yaml.safe_load(stream), indent=4, sort_keys=True))
        p = yaml_parser.from_yaml(stream)
    except yaml.YAMLError as exc:
        print(exc)

df = p.process()

print(df)
