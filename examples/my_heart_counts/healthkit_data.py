import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline.custom import CreateFeatureOperator, IterateCsvOperator, \
                                              AggregateOperator
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, DropNAOperator, \
                                              DropFeaturesOperator, SetIndexOperator, \
                                              PivotResetColumnsOperator

hkd_file_path = os.environ['MYHEARTCOUNTS_HEALTHKITDATA_PATH']
csv_folder_path = os.environ['MYHEARTCOUNTS_HEALTHKITDATA_CSV_FOLDER_PATH']

csv_pipeline = SequenceOperator([
    DropNAOperator(),
    ConvertToDatetimeOperator(
        feature_names=["endTime"],
        errors='coerce', utc=True),
    CreateFeatureOperator(
        feature_name='Date',
        feature_creator=lambda df: df['endTime'].date()),
    DropFeaturesOperator(drop_features=['startTime', 'endTime']),
    AggregateOperator(
        groupby_feature_names=["Date", "type"],
        aggregation_definition={'value': 'sum'}),
    SetIndexOperator('Date'),
    PivotResetColumnsOperator(level=1, columns='type')
])

pipeline = SequenceOperator([
    MyHeartCountsDataset(hkd_file_path),
    CreateFeatureOperator(
        feature_name='file_name',
        feature_creator=lambda df: str(df['data.csv'])),
    IterateCsvOperator(
        folder_path=csv_folder_path,
        field='file_name',
        pipeline=csv_pipeline),

])

# Returns a generator
record, csv_df = next(pipeline.process()[0])
print(record)
print(csv_df)
