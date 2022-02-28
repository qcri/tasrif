import os

from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.custom import (
    AggregateOperator,
    CreateFeatureOperator,
    ReadNestedCsvOperator,
)
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    DropFeaturesOperator,
    DropNAOperator,
    PivotResetColumnsOperator,
    SetIndexOperator,
)

mhc_file_path = os.environ["MYHEARTCOUNTS"]
csv_folder_path = os.environ["MYHEARTCOUNTS"] + "HealthKit Sleep/data.csv/"

csv_pipeline = SequenceOperator(
    [
        DropNAOperator(),
        AggregateOperator(
            groupby_feature_names=["startTime", "type"],
            aggregation_definition={"value": "sum"},
        ),
        PivotResetColumnsOperator(level=1, columns="type"),
    ]
)

pipeline = SequenceOperator(
    [
        MyHeartCountsDataset(
            mhc_file_path,
            table_name="healthkitsleep",
            nested_files_path=csv_folder_path,
            participants=5,
            nested_files_pipeline=csv_pipeline,
        ),
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
        os.path.dirname(__file__), "yaml_config/healthkit_dataset.yaml"
    )
except:
    yaml_config_path = "yaml_config/healthkit_dataset.yaml"

with open(yaml_config_path, "r") as stream:
    try:
        #         print(json.dumps(yaml.safe_load(stream), indent=4, sort_keys=True))
        p = yaml_parser.from_yaml(stream)
    except yaml.YAMLError as exc:
        print(exc)

df = p.process()

print(df)
