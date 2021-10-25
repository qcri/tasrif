import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline.custom import CreateFeatureOperator, ReadNestedCsvOperator, \
                                              AggregateOperator
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, DropNAOperator, \
                                              DropFeaturesOperator, SetIndexOperator, \
                                              PivotResetColumnsOperator

mhc_file_path = os.environ['MYHEARTCOUNTS']
csv_folder_path = os.environ['MYHEARTCOUNTS'] + 'HealthKit Sleep'

csv_pipeline = SequenceOperator([
    DropNAOperator(),
    ConvertToDatetimeOperator(
        feature_names=["endTime"],
        errors='coerce', utc=True),
    CreateFeatureOperator(
        feature_name='Date',
        feature_creator=lambda df: df['endTime'].date()),
    DropFeaturesOperator(feature_names=['startTime', 'endTime']),
    AggregateOperator(
        groupby_feature_names=["Date", "type"],
        aggregation_definition={'value': 'sum'}),
    SetIndexOperator('Date'),
    PivotResetColumnsOperator(level=1, columns='type')
])

pipeline = SequenceOperator([
    MyHeartCountsDataset(mhc_file_path, "healthkitdata"),
    CreateFeatureOperator(
        feature_name='file_name',
        feature_creator=lambda df: str(df['data.csv'])),
    ReadNestedCsvOperator(
        folder_path=csv_folder_path,
        field='file_name',
        pipeline=csv_pipeline),
])

if __name__=='__main__':
    record, csv_df = next(pipeline.process()[0])
    print(record)
    print(csv_df)
