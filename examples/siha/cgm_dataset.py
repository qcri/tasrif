"""Example on how to read CGM data from SIHA
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

siha_folder_path = os.environ.get('SIHA_PATH') or '/mnt/datafabric/qcri-hmc__profast__2020-2021-03-17T13:00:44'

pipeline = SequenceOperator([
    SihaDataset(siha_folder_path, table_name="CGM"),
    JqOperator("map({patientID} + .data.cgm[])"),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["time"], infer_datetime_format=True),
    SetIndexOperator("time"),
])

df = pipeline.process()

print(df)


