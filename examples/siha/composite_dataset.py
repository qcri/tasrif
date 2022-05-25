# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
"""Example on how to read all data from SIHA
"""
import os

from tasrif.data_readers.siha_dataset import SihaDataset
from tasrif.processing_pipeline import (
    ComposeOperator,
    NoopOperator,
    ProcessingOperator,
    SequenceOperator,
)
from tasrif.processing_pipeline.custom import CreateFeatureOperator, JqOperator
from tasrif.processing_pipeline.map_processing_operator import MapProcessingOperator
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConvertToDatetimeOperator,
    DropFeaturesOperator,
    JsonNormalizeOperator,
    SetIndexOperator,
)

siha_folder_path = os.environ.get("SIHA_PATH")


class _FlattenOperator(MapProcessingOperator):
    def _processing_function(self, arr):
        if arr:
            return arr[0]
        print(arr)
        return arr


# Rename column names
class _RenameOperator(ProcessingOperator):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def process(self, *data_frames):
        processed = []
        for data_frame in data_frames:
            data_frame = data_frame.rename(**self.kwargs)
            processed.append(data_frame)

        return processed


# %%
# base_datasets = ["EMR", "CGM", "Steps", "Distance", "Calories"]
base_datasets = SequenceOperator(
    [
        SihaDataset(folder_path=siha_folder_path, table_name="Data"),
        ComposeOperator(
            [
                JqOperator("map({patientID} + .data.emr[])"),  # EMR
                JqOperator("map({patientID} + .data.cgm[])"),  # CGM
                JqOperator(
                    'map({patientID} + .data.activities_tracker_steps[].data."activities-tracker-steps"[0])'
                ),  # Steps
                JqOperator(
                    'map({patientID} + .data.activities_tracker_distance[].data."activities-tracker-distance"[0])'
                ),  # Distance
                JqOperator(
                    'map({patientID} + .data.activities_tracker_calories[].data."activities-tracker-calories"[0])'
                ),  # Calories
            ]
        ),
        _FlattenOperator(),
        JsonNormalizeOperator(),
        _RenameOperator(columns={"time": "dateTime"}, errors="ignore")
    ]
)


df = base_datasets.process()
df

# %%
# intraday_datasets = ["HeartRateIntraday", "CaloriesIntraday", "StepsIntraday", "DistanceIntraday"]
intraday_datasets = SequenceOperator(
    [
        SihaDataset(folder_path=siha_folder_path, table_name="Data"),
        ComposeOperator(
            [
                JqOperator(
                    "map({patientID} + .data.activities_heart_intraday[].data as $item  | "
                    + '$item."activities-heart-intraday".dataset | '
                    + 'map({date: $item."activities-heart"[0].dateTime} + .) | .[])'
                ),  # HeartRateIntraday
                JqOperator(
                    "map({patientID} + .data.activities_calories_intraday[].data as $item  |"
                    + ' $item."activities-calories-intraday".dataset | '
                    + 'map({date: $item."activities-calories"[0].dateTime} + .) | .[])'
                ),  # CaloriesIntraday
                JqOperator(
                    "map({patientID} + .data.activities_steps_intraday[].data as $item  | "
                    + '$item."activities-steps-intraday".dataset | '
                    + 'map({date: $item."activities-steps"[0].dateTime} + .) | .[])'
                ),  # StepsIntraday
                JqOperator(
                    "map({patientID} + .data.activities_distance_intraday[].data as $item  |"
                    + ' $item."activities-distance-intraday".dataset | '
                    + 'map({date: $item."activities-distance"[0].dateTime} + .) | .[])'
                ),  # DistanceIntraday
            ]
        ),
        _FlattenOperator(),
        JsonNormalizeOperator()
    ]
)

df_intra = intraday_datasets.process()
df_intra

# %%
