import os
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline.pandas import DropNAOperator

dos_file_path = os.environ['MYHEARTCOUNTS_DAYONESURVEY_PATH']

pipeline = ProcessingPipeline([
    MyHeartCountsDataset(dos_file_path),
    DropNAOperator(subset=["device", "labwork"])
])

df = pipeline.process()[0]

print(df)
