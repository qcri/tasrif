import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline.pandas import DropNAOperator

dos_file_path = os.environ['MYHEARTCOUNTS_DAYONESURVEY_PATH']

pipeline = SequenceOperator([
    MyHeartCountsDataset(dos_file_path),
    DropNAOperator(subset=["device", "labwork"])
])

if __name__=='__main__':
    df = pipeline.process()[0]
    print(df)
