import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset

rf_file_path = os.environ['MYHEARTCOUNTS_RISKFACTORSURVEY_PATH']

pipeline = SequenceOperator([
    MyHeartCountsDataset(rf_file_path)
])

if __name__=='__main__':
    df = pipeline.process()[0]
    print(df)
