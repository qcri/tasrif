import os
from tasrif.processing_pipeline.pandas import JsonNormalizeOperator
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset
from tasrif.processing_pipeline.custom import CreateFeatureOperator, IterateJsonOperator

smwa_file_path = os.environ['MYHEARTCOUNTS_SIXMINUTEWALKACTIVITY_PATH']
json_folder_path = os.environ['MYHEARTCOUNTS_SIXMINUTEWALKACTIVITY_JSON_FOLDER_PATH']

json_pipeline = ProcessingPipeline([
    JsonNormalizeOperator()
])

pipeline = ProcessingPipeline([
    MyHeartCountsDataset(smwa_file_path),
    CreateFeatureOperator(
        feature_name='file_name',
        # The json filename has an extra '.0' appended to it.
        feature_creator=lambda df: str(df['pedometer_fitness.walk.items'])[:-2]),
    IterateJsonOperator(
        folder_path=json_folder_path,
        field='file_name',
        pipeline=json_pipeline),
])

# Returns a generator
record, csv_df = next(pipeline.process()[0])
print(record)
print(csv_df)
