import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline.pandas import DropNAOperator, DropDuplicatesOperator

qol_file_path = os.environ['MYHEARTCOUNTS_QUALITYOFLIFE_PATH']

pipeline = SequenceOperator([
    MyHeartCountsDataset(qol_file_path),
    DropNAOperator(),
    DropDuplicatesOperator(subset=["healthCode"], keep="last")
])

if __name__=='__main__':
    df = pipeline.process()
    print(df)
