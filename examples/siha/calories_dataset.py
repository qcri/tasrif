"""Example on how to read calories data from SIHA
"""
import os
from tasrif.processing_pipeline import (
    SequenceOperator,
    PrintOperator,
)

from tasrif.data_readers.siha_dataset import SihaDataset
from tasrif.processing_pipeline.custom import JqOperator
from tasrif.processing_pipeline.pandas import (
    JsonNormalizeOperator,
    SetIndexOperator,
    ConvertToDatetimeOperator,
    AsTypeOperator,
)

siha_folder_path = os.environ.get('SIHA_PATH')

pipeline = SequenceOperator([
    SihaDataset(folder_path=siha_folder_path, table_name="Calories"),
    PrintOperator(),
    JqOperator(
        'map({patientID} + .data.activities_tracker_calories[].data."activities-tracker-calories"[0])'
    ),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    AsTypeOperator({"value": "float32"}),
])

df = pipeline.process()

print(df)

import tasrif.yaml_parser as yaml_parser
import yaml

with open("yaml_config/calories_dataset.yaml", "r") as stream:
    try:
#         print(json.dumps(yaml.safe_load(stream), indent=4, sort_keys=True))
        p = yaml_parser.from_yaml(stream)
    except yaml.YAMLError as exc:
        print(exc)

df = p.process()

print(df)


