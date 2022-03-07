import os

from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import DropDuplicatesOperator, DropNAOperator

mhc_file_path = os.environ["MYHEARTCOUNTS"]

pipeline = SequenceOperator(
    [
        MyHeartCountsDataset(mhc_file_path, "parqsurvey"),
        DropNAOperator(
            subset=[
                "chestPain",
                "chestPainInLastMonth",
                "dizziness",
                "heartCondition",
                "jointProblem",
                "physicallyCapable",
                "prescriptionDrugs",
            ]
        ),
        DropDuplicatesOperator(subset=["healthCode"], keep="last"),
    ]
)

if __name__ == "__main__":
    df = pipeline.process()[0]
    print(df)
