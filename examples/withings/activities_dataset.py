import os
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.withings_dataset import WithingsDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator
from tasrif.processing_pipeline.custom import JsonPivotOperator

withings_data_filename = os.environ['WITHINGS_PATH']+'activities.csv'

pipeline = SequenceOperator([
    WithingsDataset(withings_data_filename, table_name="Activities"),
    JsonPivotOperator(["Data", "GPS"]),
    ConvertToDatetimeOperator(feature_names=["from", "to"], infer_datetime_format=True, utc=True),
    SetIndexOperator("from")
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


