import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline.custom import CreateFeatureOperator, ReadNestedCsvOperator, \
                                              AggregateOperator
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, DropNAOperator, \
                                              DropFeaturesOperator, SetIndexOperator, \
                                              PivotResetColumnsOperator

mhc_file_path = os.environ['MYHEARTCOUNTS']
csv_folder_path = os.environ['MYHEARTCOUNTS'] + 'HealthKit Sleep/data.csv/'

csv_pipeline = SequenceOperator([
    DropNAOperator(),
    AggregateOperator(
        groupby_feature_names=["startTime", "type"],
        aggregation_definition={'value': 'sum'}),
    PivotResetColumnsOperator(level=1, columns='type')
])

pipeline = SequenceOperator([
    MyHeartCountsDataset(mhc_file_path,
                         table_name="healthkitsleep",
                         nested_files_path=csv_folder_path, 
                         participants=5,
                         nested_files_pipeline=csv_pipeline),
])

pipeline.process()
