import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.fitbit_intraday_dataset import FitbitIntradayDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator, JsonNormalizeOperator, AsTypeOperator
from tasrif.processing_pipeline.custom import DistributedUpsampleOperator


fitbit_intraday_data_folder = os.environ['FITBIT_INTRADAY_PATH']

pipeline = SequenceOperator([
    FitbitIntradayDataset(fitbit_intraday_data_folder, table_name="Steps", num_files=5),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    AsTypeOperator({"value": "int32"}),
    DistributedUpsampleOperator("30s"),
])

df = pipeline.process()

print(df)
