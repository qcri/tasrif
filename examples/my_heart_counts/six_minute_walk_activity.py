import os

from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.custom import AggregateOperator
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    DropNAOperator,
    SortOperator,
)

mhc_file_path = os.environ["MYHEARTCOUNTS"]
json_files_path = (
    os.environ["MYHEARTCOUNTS"]
    + "Six Minute Walk Activity/pedometer_fitness.walk.items/"
)

smwa_pipeline = SequenceOperator(
    [
        ConvertToDatetimeOperator(["startDate", "endDate"], utc=True),
        DropNAOperator(),
        SortOperator(by="startDate"),
    ]
)

pipeline = SequenceOperator(
    [
        MyHeartCountsDataset(
            path_name=mhc_file_path,
            table_name="sixminutewalkactivity",
            nested_files_path=json_files_path,
            participants=5,
            nested_files_pipeline=smwa_pipeline,
        ),
        AggregateOperator(
            groupby_feature_names=["recordId"],
            aggregation_definition={"numberOfSteps": "max"},
        ),
    ]
)


if __name__ == "__main__":
    pipeline.process()
