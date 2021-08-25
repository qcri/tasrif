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
from tasrif.data_readers.fitbit_intraday_dataset import FitbitIntradayDataset
from tasrif.processing_pipeline import ProcessingPipeline, ComposeOperator
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator, JsonNormalizeOperator, AsTypeOperator, MergeOperator
from tasrif.processing_pipeline.custom import DistributedUpsampleOperator, DropIndexDuplicatesOperator, ResampleOperator, SetFeaturesValueOperator, FlattenOperator 

# %%
fitbit_intraday_data_folder = os.environ['FITBIT_INTRADAY_PATH']

# %%
distance_pipeline = ProcessingPipeline([
    FitbitIntradayDataset(fitbit_intraday_data_folder, table_name="Distance"),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    AsTypeOperator({"value": "int32"}),
    DistributedUpsampleOperator("30s"),
])
distance_pipeline.process()

# %%
sleep_pipeline = ProcessingPipeline([
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
    SetFeaturesValueOperator(features=["seconds"], value=30),
])
sleep_df = sleep_pipeline.process()

# %%
heart_rate_zones_pipeline = ProcessingPipeline([
    FitbitIntradayDataset(fitbit_intraday_data_folder,
                          table_name="Time_in_Heart_Rate_Zones"),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    AsTypeOperator({
        "value.valuesInZones.IN_DEFAULT_ZONE_3":
        "float32",
        "value.valuesInZones.IN_DEFAULT_ZONE_1":
        "float32",
        "value.valuesInZones.IN_DEFAULT_ZONE_2":
        "float32",
        "value.valuesInZones.BELOW_DEFAULT_ZONE_1":
        "float32",
    }),
])
heart_rate_zones_pipeline.process()

# %%
very_active_pipeline = ProcessingPipeline([
    FitbitIntradayDataset(fitbit_intraday_data_folder,
                          table_name="Very_Active_Minutes"),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    AsTypeOperator({"value": "int32"}),
])
very_active_df = very_active_pipeline.process()

# %%
composite_pipeline = ProcessingPipeline([
    ComposeOperator([FitbitIntradayDataset(fitbit_intraday_data_folder,
                          table_name="Very_Active_Minutes"),
                    FitbitIntradayDataset(fitbit_intraday_data_folder,
                          table_name="Lightly_Active_Minutes"),
                    FitbitIntradayDataset(fitbit_intraday_data_folder,
                          table_name="Moderately_Active_Minutes"),
                    FitbitIntradayDataset(fitbit_intraday_data_folder,
                          table_name="Sedentary_Minutes")]),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    AsTypeOperator({"value": "int32"}),
    MergeOperator(on="dateTime", how="outer"),
])
composite_pipeline.process()
