"""Example on how to read sleep data from SIHA
"""
import os

from tasrif.data_readers.siha_dataset import SihaDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator, JqOperator
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConvertToDatetimeOperator,
    DropFeaturesOperator,
    JsonNormalizeOperator,
    SetIndexOperator,
)

siha_folder_path = os.environ.get("SIHA_PATH")

pipeline = SequenceOperator(
    [
        SihaDataset(siha_folder_path, table_name="HeartRateIntraday"),
        JqOperator(
            "map({patientID} + .data.activities_heart_intraday[].data as $item  | "
            + '$item."activities-heart-intraday".dataset | '
            + 'map({date: $item."activities-heart"[0].dateTime} + .) | .[])'
        ),
        JsonNormalizeOperator(),
        CreateFeatureOperator(
            feature_name="dateTime",
            feature_creator=lambda df: df["date"] + "T" + df["time"],
        ),
        DropFeaturesOperator(["date", "time"]),
        ConvertToDatetimeOperator(
            feature_names=["dateTime"], infer_datetime_format=True
        ),
        SetIndexOperator("dateTime"),
        AsTypeOperator({"value": "float32"}),
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
        os.path.dirname(__file__), "yaml_config/heart_rate_intraday_dataset.yaml"
    )
except:
    yaml_config_path = "yaml_config/heart_rate_intraday_dataset.yaml"

with open(yaml_config_path, "r") as stream:
    try:
        #         print(json.dumps(yaml.safe_load(stream), indent=4, sort_keys=True))
        p = yaml_parser.from_yaml(stream)
    except yaml.YAMLError as exc:
        print(exc)

df = p.process()

print(df)
