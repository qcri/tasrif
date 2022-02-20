import os

from tasrif.data_readers.fitbit_intraday_dataset import FitbitIntradayDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConvertToDatetimeOperator,
    JsonNormalizeOperator,
    SetIndexOperator,
)

fitbit_intraday_data_folder = os.environ["FITBIT_INTRADAY_PATH"]

pipeline = SequenceOperator(
    [
        FitbitIntradayDataset(
            fitbit_intraday_data_folder,
            table_name="Time_in_Heart_Rate_Zones",
            num_files=5,
        ),
        JsonNormalizeOperator(),
        ConvertToDatetimeOperator(
            feature_names=["dateTime"], infer_datetime_format=True
        ),
        SetIndexOperator("dateTime"),
        AsTypeOperator(
            {
                "value.valuesInZones.IN_DEFAULT_ZONE_3": "float32",
                "value.valuesInZones.IN_DEFAULT_ZONE_1": "float32",
                "value.valuesInZones.IN_DEFAULT_ZONE_2": "float32",
                "value.valuesInZones.BELOW_DEFAULT_ZONE_1": "float32",
            }
        ),
    ]
)

df = pipeline.process()

print(df)

import yaml

import tasrif.yaml_parser as yaml_parser

with open("yaml_config/time_in_heart_rate_zones_dataset.yaml", "r") as stream:
    try:
        #         print(json.dumps(yaml.safe_load(stream), indent=4, sort_keys=True))
        p = yaml_parser.from_yaml(stream)
    except yaml.YAMLError as exc:
        print(exc)

df = p.process()

print(df)
