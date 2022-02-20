import os

from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConvertToDatetimeOperator,
    SetIndexOperator,
)

pipeline = SequenceOperator(
    [
        WithingsDataset(
            [
                os.environ["WITHINGS_PATH"] + "raw_tracker_latitude.csv",
                os.environ["WITHINGS_PATH"] + "raw_tracker_longitude.csv",
            ],
            table_name="Lat_Long",
        ),
        ConvertToDatetimeOperator(
            feature_names=["from", "to"], infer_datetime_format=True, utc=True
        ),
        SetIndexOperator("from"),
        AsTypeOperator({"latitude": "float32", "longitude": "float32"}),
    ]
)

df = pipeline.process()

print(df)
