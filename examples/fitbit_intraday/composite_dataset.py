import os
from tasrif.processing_pipeline import ProcessingPipeline, ComposeOperator
from tasrif.data_readers.fitbit_intraday_dataset import FitbitIntradayDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator, JsonNormalizeOperator, AsTypeOperator, MergeOperator

fitbit_intraday_data_folder = os.environ['FITBIT_INTRADAY_PATH']

composite_pipeline = ProcessingPipeline([
    ComposeOperator([FitbitIntradayDataset(fitbit_intraday_data_folder,
                                           table_name="Very_Active_Minutes"),
                     FitbitIntradayDataset(fitbit_intraday_data_folder,
                                           table_name="Lightly_Active_Minutes"),
                     FitbitIntradayDataset(fitbit_intraday_data_folder,
                                           table_name="Moderately_Active_Minutes"),
                     FitbitIntradayDataset(fitbit_intraday_data_folder,
                                           table_name="Sedentary_Minutes")]),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    AsTypeOperator({"value": "int32"}),
    MergeOperator(on="dateTime", how="outer"),
])

df = composite_pipeline.process()

print(df)
