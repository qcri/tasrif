import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.fitbit_intraday_dataset import FitbitIntradayDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator, JsonNormalizeOperator
from tasrif.processing_pipeline.custom import DropIndexDuplicatesOperator, ResampleOperator, SetFeaturesValueOperator


fitbit_intraday_data_folder = os.environ['FITBIT_INTRADAY_PATH']

pipeline = SequenceOperator([
    FitbitIntradayDataset(fitbit_intraday_data_folder, table_name="Sleep"),
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
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    DropIndexDuplicatesOperator(keep="first"),
    ResampleOperator("30s", "ffill"),
    SetFeaturesValueOperator(feature_names=["seconds"], value=30),
])

df = pipeline.process()

print(df)
