"""Example on how to read sleep data from SIHA
"""
import os
from tasrif.processing_pipeline import (
    ProcessingPipeline,
)

from tasrif.data_readers.siha_dataset import SihaDataset
from tasrif.processing_pipeline.custom import JqOperator
from tasrif.processing_pipeline.pandas import (
    JsonNormalizeOperator,
    SetIndexOperator,
    ConvertToDatetimeOperator,
    AsTypeOperator,
)

siha_folder_path = os.environ['SIHA_PATH']

pipeline = ProcessingPipeline([
    SihaDataset(siha_folder_path, table_name="VeryActiveMinutes"),
    JqOperator(
        'map({patientID} + .data.activities_tracker_minutesVeryActive[].data.'
        +
        '"activities-tracker-minutesVeryActive"[0])'
    ),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    AsTypeOperator({"value": "float32"}),
])

df = pipeline.process()

print(df)