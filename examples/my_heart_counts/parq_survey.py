import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline.pandas import DropNAOperator, DropDuplicatesOperator

parq_file_path = os.environ['MYHEARTCOUNTS_PARQSURVEY_PATH']

pipeline = SequenceOperator([
    MyHeartCountsDataset(parq_file_path),
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

if __name__=='__main__':
    df = pipeline.process()[0]
    print(df)
