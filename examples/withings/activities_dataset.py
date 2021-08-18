import os
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator
from tasrif.processing_pipeline.custom import JsonPivotOperator

# +
withings_data_filename = os.environ['WITHINGS_PATH']+'activities.csv'
# -

pipeline = ProcessingPipeline([
    WithingsDataset(withings_data_filename, table_name="Activities"),
    JsonPivotOperator(["Data", "GPS"]),
    ConvertToDatetimeOperator(feature_names=["from", "to"], infer_datetime_format=True, utc=True),
    SetIndexOperator("from")
])

df = pipeline.process()

print(df)