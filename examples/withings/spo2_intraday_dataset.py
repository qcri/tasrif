import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator, AsTypeOperator

# +
withings_data_filename = os.environ['WITHINGS_PATH']+'raw_tracker_auto_spo2.csv'
# -

pipeline = SequenceOperator([
    WithingsDataset(withings_data_filename, table_name="SpO2"),
    ConvertToDatetimeOperator(feature_names=["from", "to"], infer_datetime_format=True, utc=True),
    SetIndexOperator("from"),
    AsTypeOperator({"sp_o2": "int32"})
])

df = pipeline.process()

print(df)