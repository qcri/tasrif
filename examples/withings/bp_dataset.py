import os
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator, AsTypeOperator

# +
withings_data_filename = os.environ['WITHINGS_PATH']+'bp.csv'
# -

pipeline = ProcessingPipeline([
    WithingsDataset(withings_data_filename, table_name="Blood_Pressure"),
    ConvertToDatetimeOperator(feature_names=["Date"], infer_datetime_format=True),
    SetIndexOperator("Date"),
    AsTypeOperator({"Heart rate": "int32", "Systolic": "float32", "Diastolic": "float32"})
])

df = pipeline.process()

print(df)