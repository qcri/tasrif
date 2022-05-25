"""Example on how to read sleep data from SIHA
"""
import os

from tasrif.data_readers.siha_dataset import SihaDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.custom import JqOperator
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    JsonNormalizeOperator,
    SetIndexOperator,
)

siha_folder_path = os.environ.get("SIHA_PATH")

pipeline = SequenceOperator(
    [
        SihaDataset(siha_folder_path, table_name="Data"),
        JqOperator(
            "map({patientID} + (.data.sleep[].data as $data | "
            + "($data.sleep | map(.) | .[]) | . * {levels:  {overview : ($data.summary//{})}})) |  "
            + "map (if .levels.data != null then . else .levels += {data: []} end) | "
            + "map(. + {type, dateOfSleep, minutesAsleep, logId, startTime, endTime, duration, isMainSleep,"
            + " minutesToFallAsleep, minutesAwake, minutesAfterWakeup, timeInBed, efficiency, infoCode})"
        ),
        JsonNormalizeOperator(
            record_path=["levels", "data"],
            meta=[
                "patientID",
                "logId",
                "dateOfSleep",
                "startTime",
                "endTime",
                "duration",
                "isMainSleep",
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
                ["levels", "overview", "totalTimeInBed"],
                ["levels", "overview", "totalMinutesAsleep"],
                ["levels", "overview", "stages", "rem"],
                ["levels", "overview", "stages", "deep"],
                ["levels", "overview", "stages", "light"],
                ["levels", "overview", "stages", "wake"],
            ],
            errors="ignore",
        ),
        ConvertToDatetimeOperator(
            feature_names=["dateTime"], infer_datetime_format=True
        ),
        SetIndexOperator("dateTime"),
    ]
)

df = pipeline.process()

print(df)
