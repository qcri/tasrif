import os
from tasrif.processing_pipeline import SequenceOperator, ComposeOperator
from tasrif.data_readers.fitbit_intraday_dataset import FitbitIntradayDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator, JsonNormalizeOperator, AsTypeOperator, MergeOperator

fitbit_intraday_data_folder = os.environ['FITBIT_INTRADAY_PATH']

composite_pipeline = SequenceOperator([
    ComposeOperator([FitbitIntradayDataset(fitbit_intraday_data_folder,
                                           table_name="Very_Active_Minutes",
                                           num_files=5),
                     FitbitIntradayDataset(fitbit_intraday_data_folder,
                                           table_name="Lightly_Active_Minutes",
                                           num_files=5),
                     FitbitIntradayDataset(fitbit_intraday_data_folder,
                                           table_name="Moderately_Active_Minutes",
                                           num_files=5),
                     FitbitIntradayDataset(fitbit_intraday_data_folder,
                                           table_name="Sedentary_Minutes",
                                           num_files=5)]),
    JsonNormalizeOperator(),
    ConvertToDatetimeOperator(feature_names=["dateTime"],
                              infer_datetime_format=True),
    SetIndexOperator("dateTime"),
    AsTypeOperator({"value": "int32"}),
    MergeOperator(on="dateTime", how="outer"),
])

df = composite_pipeline.process()

print(df)

import tasrif.yaml_parser as yaml_parser
import yaml

with open("yaml_config/composite_dataset.yaml", "r") as stream:
    try:
#         print(json.dumps(yaml.safe_load(stream), indent=4, sort_keys=True))
        p = yaml_parser.from_yaml(stream)
    except yaml.YAMLError as exc:
        print(exc)

df = p.process()

print(df)
