"""Example on how to read sleep data from SIHA
"""
import os

from tasrif.processing_pipeline import (
    SequenceOperator,
)

from tasrif.data_readers.siha_dataset import SihaDataset
from tasrif.processing_pipeline.custom import JqOperator
from tasrif.processing_pipeline.pandas import (
    JsonNormalizeOperator,
    SetIndexOperator,
    ConvertToDatetimeOperator,
)

siha_folder_path = os.environ['SIHA_PATH']

pipeline = SequenceOperator([
    SihaDataset(siha_folder_path, table_name="Sleep"),
    JqOperator("map({patientID} + .data.sleep[].data)"),
    JsonNormalizeOperator(
        record_path=["sleep"],
        meta=[
            "patientID",
            ["summary", "stages", "rem"],
            ["summary", "stages", "deep"],
            ["summary", "stages", "light"],
            ["summary", "stages", "wake"],
            ["summary", "totalMinutesAsleep"],
            ["summary", "totalTimeInBed"],
        ],
        errors="ignore",
    ),
    ConvertToDatetimeOperator(feature_names=["dateOfSleep"],
                              infer_datetime_format=True),
    SetIndexOperator("dateOfSleep"),
])

df = pipeline.process()

print(df)
