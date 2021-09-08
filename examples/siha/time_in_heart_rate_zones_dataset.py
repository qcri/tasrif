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
    SihaDataset(siha_folder_path, table_name="TimeInHeartRateZones"),
    JqOperator(
        'map({patientID} + .data.activities_heart[].data."activities-heart"[] as $item |'
        +
        "{dateTime: $item.dateTime, restingHeartRate: $item.value.restingHeartRate} +"
        +
        "reduce $item.value.heartRateZones[] as $i ({}; .[$i.name] = $i.minutes))"
    ),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
])

df = pipeline.process()

print(df)
