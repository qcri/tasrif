import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset

cds_file_path = os.environ['MYHEARTCOUNTS_CARDIODIETSURVEY_PATH']

pipeline = SequenceOperator([
    MyHeartCountsDataset(cds_file_path)
])

df = pipeline.process()

print(df)
