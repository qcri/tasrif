import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.fitbit_intraday_dataset import FitbitIntradayDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator, JsonNormalizeOperator, AsTypeOperator


fitbit_intraday_data_folder = os.environ['FITBIT_INTRADAY_PATH']

pipeline = SequenceOperator([
    FitbitIntradayDataset(fitbit_intraday_data_folder,
                          table_name="Lightly_Active_Minutes"),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    AsTypeOperator({"value": "int32"}),
])

df = pipeline.process()

print(df)
