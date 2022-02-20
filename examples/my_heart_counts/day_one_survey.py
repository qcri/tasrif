import os

from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import DropNAOperator

mhc_file_path = os.environ["MYHEARTCOUNTS"]

pipeline = SequenceOperator(
    [
        MyHeartCountsDataset(mhc_file_path, "dayonesurvey"),
        DropNAOperator(subset=["device", "labwork"]),
    ]
)

if __name__ == "__main__":
    df = pipeline.process()[0]
    print(df)
