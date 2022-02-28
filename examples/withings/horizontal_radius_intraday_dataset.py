import os

from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConvertToDatetimeOperator,
    ReplaceOperator,
    SetIndexOperator,
)

# +
withings_data_filename = (
    os.environ["WITHINGS_PATH"] + "raw_tracker_horizontal-radius.csv"
)
# -

pipeline = SequenceOperator(
    [
        WithingsDataset(withings_data_filename, table_name="Horizontal_Radius"),
        ConvertToDatetimeOperator(
            feature_names=["from", "to"], infer_datetime_format=True, utc=True
        ),
        SetIndexOperator("from"),
        AsTypeOperator({"horizontal__radius": "int32"}, errors="ignore"),
    ]
)

df = pipeline.process()

print(df)
