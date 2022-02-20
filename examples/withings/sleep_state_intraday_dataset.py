import os

from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConvertToDatetimeOperator,
    SetIndexOperator,
)

# +
withings_data_filename = os.environ["WITHINGS_PATH"] + "raw_tracker_sleep-state.csv"
# -

pipeline = SequenceOperator(
    [
        WithingsDataset(withings_data_filename, table_name="Sleep_State"),
        ConvertToDatetimeOperator(
            feature_names=["from", "to"], infer_datetime_format=True, utc=True
        ),
        SetIndexOperator("from"),
        AsTypeOperator({"sleep__state": "int32"}),
    ]
)

df = pipeline.process()

print(df)
