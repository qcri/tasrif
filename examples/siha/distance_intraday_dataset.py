"""Example on how to read sleep data from SIHA
"""
import os
from tasrif.processing_pipeline import (
    SequenceOperator,
)

from tasrif.data_readers.siha_dataset import SihaDataset
from tasrif.processing_pipeline.custom import JqOperator, CreateFeatureOperator
from tasrif.processing_pipeline.pandas import (
    JsonNormalizeOperator,
    SetIndexOperator,
    ConvertToDatetimeOperator,
    AsTypeOperator,
    DropFeaturesOperator
)

siha_folder_path = os.environ['SIHA_PATH']

pipeline = SequenceOperator([
    SihaDataset(siha_folder_path, table_name="DistanceIntraday"),
    JqOperator(
        "map({patientID} + .data.activities_distance_intraday[].data as $item  |"
        +
        ' $item."activities-distance-intraday".dataset | '
        +
        'map({date: $item."activities-distance"[0].dateTime} + .) | .[])'
    ),
    JsonNormalizeOperator(),
    CreateFeatureOperator(
        feature_name="dateTime",
        feature_creator=lambda df: df["date"] + "T" + df["time"],
    ),
    DropFeaturesOperator(["date", "time"]),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    AsTypeOperator({"value": "float32"}),
])

df = pipeline.process()

print(df)
