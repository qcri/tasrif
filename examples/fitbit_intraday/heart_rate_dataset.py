import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.fitbit_intraday_dataset import FitbitIntradayDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator, JsonNormalizeOperator, AsTypeOperator
from tasrif.processing_pipeline.custom import DropIndexDuplicatesOperator, ResampleOperator


fitbit_intraday_data_folder = os.environ['FITBIT_INTRADAY_PATH']

pipeline = SequenceOperator([
    FitbitIntradayDataset(fitbit_intraday_data_folder,
                          table_name="Heart_Rate"),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    AsTypeOperator({
        "value.bpm": "int32",
        "value.confidence": "int32"
    }),
    DropIndexDuplicatesOperator(keep="first"),
    ResampleOperator("30s", {
        "value.bpm": "mean",
        "value.confidence": "mean"
    }),
])

df = pipeline.process()

print(df)
