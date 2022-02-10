import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.fitbit_interday_dataset import FitbitInterdayDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator

interday_folder_path = os.environ.get('FITBIT_INTERDAY_PATH', '/mnt/data/fitbit-data/')

pipeline = SequenceOperator([
    FitbitInterdayDataset(interday_folder_path, table_name="Activities"),
    ConvertToDatetimeOperator(feature_names=['Date'],
                              infer_datetime_format=True),
    SetIndexOperator('Date')
])

df = pipeline.process()

print(df)

import tasrif.yaml_parser as yaml_parser
import yaml

with open("yaml_config/activities_dataset.yaml", "r") as stream:
    try:
#         print(json.dumps(yaml.safe_load(stream), indent=4, sort_keys=True))
        p = yaml_parser.from_yaml(stream)
    except yaml.YAMLError as exc:
        print(exc)

df = p.process()

print(df)


