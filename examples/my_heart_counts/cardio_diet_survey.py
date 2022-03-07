import os

from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline import SequenceOperator

mhc_file_path = os.environ["MYHEARTCOUNTS"]

pipeline = SequenceOperator([MyHeartCountsDataset(mhc_file_path, "cardiodietsurvey")])

if __name__ == "__main__":
    df = pipeline.process()
    print(df)
