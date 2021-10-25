import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset

mhc_file_path = os.environ['MYHEARTCOUNTS']

pipeline = SequenceOperator([
    MyHeartCountsDataset(mhc_file_path, "riskfactorsurvey")
])

if __name__ == '__main__':
    df = pipeline.process()[0]
    print(df)
