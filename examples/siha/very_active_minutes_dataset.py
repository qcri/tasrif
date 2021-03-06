"""Example on how to read sleep data from SIHA
"""
import os

from tasrif.data_readers.siha_dataset import SihaDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.custom import JqOperator
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConvertToDatetimeOperator,
    JsonNormalizeOperator,
    SetIndexOperator,
)

siha_folder_path = (
    os.environ.get("SIHA_PATH")
    or "/mnt/datafabric/qcri-hmc__profast__2020-2021-03-17T13:00:44"
)

pipeline = SequenceOperator(
    [
        SihaDataset(siha_folder_path, table_name="VeryActiveMinutes"),
        JqOperator(
            "map({patientID} + .data.activities_tracker_minutesVeryActive[].data."
            + '"activities-tracker-minutesVeryActive"[0])'
        ),
        JsonNormalizeOperator(),
        ConvertToDatetimeOperator(
            feature_names=["dateTime"], infer_datetime_format=True
        ),
        SetIndexOperator("dateTime"),
        AsTypeOperator({"value": "float32"}),
    ]
)

df = pipeline.process()

print(df)
