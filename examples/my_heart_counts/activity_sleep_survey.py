import os
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.my_heart_counts import ActivitySleepSurveyDataset
from tasrif.processing_pipeline.pandas import DropNAOperator, DropDuplicatesOperator

cds_file_path = os.environ['MYHEARTCOUNTS_ACTIVITYSLEEPSURVEY_PATH']

pipeline = ProcessingPipeline([
    ActivitySleepSurveyDataset(cds_file_path),
    DropNAOperator(),
    DropDuplicatesOperator(subset=["healthCode"], keep="last")
])

df = pipeline.process()

print(df)
