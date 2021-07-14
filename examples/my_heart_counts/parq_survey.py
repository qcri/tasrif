import os
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.my_heart_counts import PARQSurveyDataset
from tasrif.processing_pipeline.pandas import DropNAOperator, DropDuplicatesOperator

parq_file_path = os.environ['MYHEARTCOUNTS_PARQSURVEY_PATH']

pipeline = ProcessingPipeline([
    PARQSurveyDataset(parq_file_path),
    DropNAOperator(subset=[
                    "chestPain",
                    "chestPainInLastMonth",
                    "dizziness",
                    "heartCondition",
                    "jointProblem",
                    "physicallyCapable",
                    "prescriptionDrugs",
                ]),
    DropDuplicatesOperator(subset=["healthCode"], keep="last")
])

df = pipeline.process()[0]

print(df)
