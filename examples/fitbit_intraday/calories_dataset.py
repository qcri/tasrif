import os

from tasrif.data_readers.fitbit_intraday_dataset import FitbitIntradayDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.custom import DistributedUpsampleOperator
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConvertToDatetimeOperator,
    JsonNormalizeOperator,
    SetIndexOperator,
)

fitbit_intraday_data_folder = os.environ["FITBIT_INTRADAY_PATH"]

pipeline = SequenceOperator(
    [
        FitbitIntradayDataset(fitbit_intraday_data_folder, table_name="Calories"),
        JsonNormalizeOperator(),
        ConvertToDatetimeOperator(
            feature_names=["dateTime"], infer_datetime_format=True
        ),
        SetIndexOperator("dateTime"),
        AsTypeOperator({"value": "float32"}),
        DistributedUpsampleOperator("30s"),
    ]
)

df = pipeline.process()

print(df)

import yaml

import tasrif.yaml_parser as yaml_parser

with open("yaml_config/calories_dataset.yaml", "r") as stream:
    try:
        #         print(json.dumps(yaml.safe_load(stream), indent=4, sort_keys=True))
        p = yaml_parser.from_yaml(stream)
    except yaml.YAMLError as exc:
        print(exc)

df = p.process()

print(df)
