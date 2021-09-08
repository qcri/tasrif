# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import os
from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator, AsTypeOperator, FillNAOperator
from tasrif.processing_pipeline.custom import JsonPivotOperator

# %%
steps_pipeline = SequenceOperator([
    WithingsDataset(os.environ['WITHINGS_PATH']+'raw_tracker_steps.csv', table_name="Steps"),
    ConvertToDatetimeOperator(feature_names=["from", "to"], infer_datetime_format=True, utc=True),
    SetIndexOperator("from"),
    AsTypeOperator({"steps": "int32"})
])

steps_pipeline.process()

# %%
height_pipeline = SequenceOperator([
    WithingsDataset(os.environ['WITHINGS_PATH']+'height.csv', table_name="Height"),
    ConvertToDatetimeOperator(feature_names=["Date"], infer_datetime_format=True),
    SetIndexOperator("Date"),
    AsTypeOperator({"height": "float32"})
])

height_pipeline.process()

# %%
activities_pipeline = SequenceOperator([
    WithingsDataset(os.environ['WITHINGS_PATH']+'activities.csv', table_name="Activities"),
    JsonPivotOperator(["Data", "GPS"]),
    ConvertToDatetimeOperator(feature_names=["from", "to"], infer_datetime_format=True, utc=True),
    SetIndexOperator("from")
])

activities_pipeline.process()

# %%
bp_pipeline = SequenceOperator([
    WithingsDataset(os.environ['WITHINGS_PATH']+'bp.csv', table_name="Blood_Pressure"),
    ConvertToDatetimeOperator(feature_names=["Date"], infer_datetime_format=True),
    SetIndexOperator("Date"),
    AsTypeOperator({"Heart rate": "int32", "Systolic": "float32", "Diastolic": "float32"})
])

bp_pipeline.process()

# %%
latlong_pipeline = SequenceOperator([
    WithingsDataset([os.environ['WITHINGS_PATH']+'raw_tracker_latitude.csv', os.environ['WITHINGS_PATH']+'raw_tracker_longitude.csv'], table_name="Lat_Long"),
    ConvertToDatetimeOperator(feature_names=["from", "to"], infer_datetime_format=True, utc=True),
    SetIndexOperator("from"),
    AsTypeOperator({"latitude": "float32", "longitude": "float32"})
])

latlong_pipeline.process()

