import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.fitbit_interday_dataset import FitbitInterdayDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator, AggregateOperator

interday_folder_path = os.environ.get('FITBIT_INTERDAY_PATH', '/mnt/data/fitbit-data/')

pipeline = SequenceOperator([
    FitbitInterdayDataset(interday_folder_path, table_name="Sleep"),
    ConvertToDatetimeOperator(feature_names=['Start Time', 'End Time'],
                                infer_datetime_format=True),
    CreateFeatureOperator(
        feature_name="Date",
        feature_creator=lambda df: df['End Time'].date()),
    AggregateOperator(groupby_feature_names="Date",
                        aggregation_definition={
                            'Minutes Asleep': 'sum',
                            'Minutes Awake': 'sum',
                            'Number of Awakenings': 'sum',
                            'Time in Bed': 'sum',
                            'Minutes REM Sleep': 'sum',
                            'Minutes Light Sleep': 'sum',
                            'Minutes Deep Sleep': 'sum'
                        }),
    SetIndexOperator('Date')
])

df = pipeline.process()

print(df)
