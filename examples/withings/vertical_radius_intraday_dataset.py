import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator, AsTypeOperator

# +
withings_data_filename = os.environ['WITHINGS_PATH']+'raw_tracker_vertical-radius.csv'
# -

pipeline = SequenceOperator([
    WithingsDataset(withings_data_filename, table_name="Vertical_Radius"),
    ConvertToDatetimeOperator(feature_names=["from", "to"], infer_datetime_format=True, utc=True),
    SetIndexOperator("from"),
    AsTypeOperator({"vertical__radius": "int32", errors='ignore'})
])

df = pipeline.process()

print(df)
