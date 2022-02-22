import os

from tasrif.data_readers.fitbit_intraday_dataset import FitbitIntradayDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.custom import (
    DropIndexDuplicatesOperator,
    ResampleOperator,
    SetFeaturesValueOperator,
)
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    JsonNormalizeOperator,
    SetIndexOperator,
)

fitbit_intraday_data_folder = os.environ["FITBIT_INTRADAY_PATH"]

pipeline = SequenceOperator(
    [
        FitbitIntradayDataset(
            fitbit_intraday_data_folder, table_name="Sleep", num_files=5
        ),
        JsonNormalizeOperator(
            record_path=["levels", "data"],
            meta=[
                "logId",
                "dateOfSleep",
                "startTime",
                "endTime",
                "duration",
                "minutesToFallAsleep",
                "minutesAsleep",
                "minutesAwake",
                "minutesAfterWakeup",
                "timeInBed",
                "efficiency",
                "type",
                "infoCode",
                ["levels", "summary", "deep", "count"],
                ["levels", "summary", "deep", "minutes"],
                ["levels", "summary", "deep", "thirtyDayAvgMinutes"],
                ["levels", "summary", "wake", "count"],
                ["levels", "summary", "wake", "minutes"],
                ["levels", "summary", "wake", "thirtyDayAvgMinutes"],
                ["levels", "summary", "light", "count"],
                ["levels", "summary", "light", "minutes"],
                ["levels", "summary", "light", "thirtyDayAvgMinutes"],
                ["levels", "summary", "rem", "count"],
                ["levels", "summary", "rem", "minutes"],
                ["levels", "summary", "rem", "thirtyDayAvgMinutes"],
            ],
            errors="ignore",
        ),
        ConvertToDatetimeOperator(
            feature_names=["dateTime"], infer_datetime_format=True
        ),
        SetIndexOperator("dateTime"),
        DropIndexDuplicatesOperator(keep="first"),
        ResampleOperator("30s", "ffill"),
        SetFeaturesValueOperator(feature_names=["seconds"], value=30),
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
        os.path.dirname(__file__), "yaml_config/sleep_dataset.yaml"
    )
except:
    yaml_config_path = "yaml_config/sleep_dataset.yaml"

with open(yaml_config_path, "r") as stream:
    try:
        #         print(json.dumps(yaml.safe_load(stream), indent=4, sort_keys=True))
        p = yaml_parser.from_yaml(stream)
    except yaml.YAMLError as exc:
        print(exc)

df = p.process()

print(df)
