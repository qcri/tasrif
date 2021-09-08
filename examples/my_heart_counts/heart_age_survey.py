import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline.pandas import DropNAOperator, DropDuplicatesOperator

has_file_path = os.environ['MYHEARTCOUNTS_HEARTAGESURVEY_PATH']

pipeline = SequenceOperator([
    MyHeartCountsDataset(has_file_path),
    DropNAOperator(),
    DropDuplicatesOperator(subset=["healthCode"], keep="last")
])

df = pipeline.process()

print(df)
