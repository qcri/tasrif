import os
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.my_heart_counts import RiskFactorSurveyDataset

rf_file_path = os.environ['MYHEARTCOUNTS_RISKFACTORSURVEY_PATH']

pipeline = ProcessingPipeline([
    RiskFactorSurveyDataset(rf_file_path)
])

df = pipeline.process()[0]

print(df)
