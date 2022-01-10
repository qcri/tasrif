import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline.custom import AggregateOperator
from tasrif.processing_pipeline.pandas import DropNAOperator, SortOperator, ConvertToDatetimeOperator

mhc_file_path = os.environ['MYHEARTCOUNTS']
json_files_path = os.environ['MYHEARTCOUNTS'] + 'Six Minute Walk Activity/'

smwa_pipeline = SequenceOperator([ConvertToDatetimeOperator(['startDate', 'endDate'], utc=True),
                                  DropNAOperator(),
                                  SortOperator(by='startDate')])

smwa_pipeline = SequenceOperator([
    MyHeartCountsDataset(path_name=mhc_file_path, table_name='sixminutewalkactivity',
                         nested_files_path=json_files_path, participants=5,
                         nested_files_pipeline=smwa_pipeline),
    AggregateOperator(
        groupby_feature_names=["recordId"],
        aggregation_definition={'numberOfSteps': 'max'}
    )
])

smwa_pipeline.process()
