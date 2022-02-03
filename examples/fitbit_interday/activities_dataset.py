import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.fitbit_interday_dataset import FitbitInterdayDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator

os.environ['FITBIT_INTERDAY_PATH'] = '/mnt/data/fitbit-data/'
interday_folder_path = os.environ['FITBIT_INTERDAY_PATH']

pipeline = SequenceOperator([
    FitbitInterdayDataset(interday_folder_path, table_name="Activities"),
    ConvertToDatetimeOperator(feature_names=['Date'],
                              infer_datetime_format=True),
    SetIndexOperator('Date')
])

df = pipeline.process()

print(df)


