import os
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.my_heart_counts import QualityOfLifeSurveyDataset
from tasrif.processing_pipeline.pandas import DropNAOperator, DropDuplicatesOperator

qol_file_path = os.environ['MYHEARTCOUNTS_QUALITYOFLIFE_PATH']

pipeline = ProcessingPipeline([
    QualityOfLifeSurveyDataset(qol_file_path),
    DropNAOperator(),
    DropDuplicatesOperator(subset=["healthCode"], keep="last")
])

df = pipeline.process()

print(df)
