import os
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.my_heart_counts import CardioDietSurveyDataset

cds_file_path = os.environ['MYHEARTCOUNTS_CARDIODIETSURVEY_PATH']

pipeline = ProcessingPipeline([
    CardioDietSurveyDataset(cds_file_path)
])

df = pipeline.process()

print(df)
