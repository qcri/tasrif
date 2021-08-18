import os
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline.custom import CreateFeatureOperator, AggregateOperator
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator

withings_data_filename = os.environ['WITHINGS_PATH']+'sleep.csv'

pipeline = ProcessingPipeline([
    WithingsDataset(withings_data_filename, table_name="Sleep"),
    ConvertToDatetimeOperator(feature_names=["from", "to"],
                              infer_datetime_format=True),
    CreateFeatureOperator(
        feature_name="Date",
        feature_creator=lambda df: df['to'].date()),
    AggregateOperator(groupby_feature_names="Date",
                      aggregation_definition={
                          'Heart rate (min)': 'mean',
                          'Heart rate (max)': 'mean',
                          'Average heart rate': 'mean',
                          'Duration to sleep (s)': 'sum',
                          'Duration to wake up (s)': 'sum',
                          'Snoring (s)': 'sum',
                          'Snoring episodes': 'sum',
                          'rem (s)': 'sum',
                          'light (s)': 'sum',
                          'deep (s)': 'sum',
                          'awake (s)': 'sum',
                      }),
    SetIndexOperator('Date'),
])

df = pipeline.process()

print(df)
