import os
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset

cds_file_path = os.environ['MYHEARTCOUNTS_CARDIODIETSURVEY_PATH']

pipeline = ProcessingPipeline([
    MyHeartCountsDataset(cds_file_path)
])

df = pipeline.process()

print(df)
