import os
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator, AsTypeOperator

# +
withings_data_filename = os.environ['WITHINGS_PATH']+'raw_tracker_elevation.csv'
# -

pipeline = ProcessingPipeline([
    WithingsDataset(withings_data_filename, table_name="Elevation"),
    ConvertToDatetimeOperator(feature_names=["from", "to"], infer_datetime_format=True, utc=True),
    SetIndexOperator("from"),
    AsTypeOperator({"elevation": "int32"})
])

df = pipeline.process()

print(df)
