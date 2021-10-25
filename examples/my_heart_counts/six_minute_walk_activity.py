import os
from tasrif.processing_pipeline.pandas import JsonNormalizeOperator
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline.custom import CreateFeatureOperator, IterateJsonOperator

mhc_file_path = os.environ['MYHEARTCOUNTS']
json_files_path = os.environ['MYHEARTCOUNTS'] + 'Six Minute Walk Activity'


json_pipeline = SequenceOperator([
    JsonNormalizeOperator()
])

pipeline = SequenceOperator([
    MyHeartCountsDataset(mhc_file_path, "sixminutewalkactivity"),
    CreateFeatureOperator(
        feature_name='file_name',
        # The json filename has an extra '.0' appended to it.
        feature_creator=lambda df: str(df['pedometer_fitness.walk.items'])[:-2]),
    IterateJsonOperator(
        folder_path=json_files_path,
        field='file_name',
        pipeline=json_pipeline),
])

if __name__ == '__main__':
    record, csv_df = next(pipeline.process()[0])
    print(record)
    print(csv_df)
