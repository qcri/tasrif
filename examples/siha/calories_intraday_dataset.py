"""Example on how to read intraday calories data from SIHA
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
        SihaDataset(siha_folder_path, table_name="Data"),
        JqOperator(
            "map({patientID} + .data.activities_calories_intraday[].data as $item  |"
            + ' $item."activities-calories-intraday".dataset | '
            + 'map({date: $item."activities-calories"[0].dateTime} + .) | .[])'
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
