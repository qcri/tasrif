"""Example on how to read sleep data from SIHA
"""
import os

from tasrif.data_readers.siha_dataset import SihaDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.custom import (
    CreateFeatureOperator, 
    JqOperator, 
    AggregateOperator,
    LinearFitOperator
)
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConvertToDatetimeOperator,
    DropFeaturesOperator,
    JsonNormalizeOperator,
    SetIndexOperator,
)

# +
import json 
 
json_str = {
    "data": [
        {
            "Cardio": 62,
            "Fat Burn": 332,
            "Light": 972,
            "Peak": 185,
            "date": "2022-03-28"
        },
        {
            "Cardio": 324,
            "Fat Burn": 487,
            "Light": 994,
            "Peak": 110,
            "date": "2022-03-29"
        },
        {
            "Cardio": 419,
            "Fat Burn": 42,
            "Light": 911,
            "Peak": 192,
            "date": "2022-03-30"
        },
        {
            "Cardio": 193,
            "Fat Burn": 195,
            "Light": 1149,
            "Peak": 177,
            "date": "2022-03-31"
        },
        {
            "Cardio": 170,
            "Fat Burn": 161,
            "Light": 680,
            "Peak": 26,
            "date": "2022-04-01"
        },
        {
            "Cardio": 168,
            "Fat Burn": 33,
            "Light": 1230,
            "Peak": 148,
            "date": "2022-04-02"
        },
        {
            "Cardio": 492,
            "Fat Burn": 79,
            "Light": 951,
            "Peak": 111,
            "date": "2022-04-03"
        },
        {
            "Cardio": 335,
            "Fat Burn": 20,
            "Light": 1107,
            "Peak": 66,
            "date": "2022-04-04"
        },
        {
            "Cardio": 36,
            "Fat Burn": 27,
            "Light": 1307,
            "Peak": 51,
            "date": "2022-04-05"
        },
        {
            "Cardio": 387,
            "Fat Burn": 445,
            "Light": 1215,
            "Peak": 87,
            "date": "2022-04-06"
        },
        {
            "Cardio": 184,
            "Fat Burn": 255,
            "Light": 737,
            "Peak": 146,
            "date": "2022-04-07"
        },
        {
            "Cardio": 143,
            "Fat Burn": 322,
            "Light": 1247,
            "Peak": 71,
            "date": "2022-04-08"
        },
        {
            "Cardio": 298,
            "Fat Burn": 457,
            "Light": 962,
            "Peak": 195,
            "date": "2022-04-09"
        },
        {
            "Cardio": 312,
            "Fat Burn": 43,
            "Light": 779,
            "Peak": 199,
            "date": "2022-04-10"
        },
        {
            "Cardio": 441,
            "Fat Burn": 289,
            "Light": 1381,
            "Peak": 117,
            "date": "2022-04-11"
        },
        {
            "Cardio": 259,
            "Fat Burn": 142,
            "Light": 1090,
            "Peak": 74,
            "date": "2022-04-12"
        },
        {
            "Cardio": 309,
            "Fat Burn": 379,
            "Light": 986,
            "Peak": 171,
            "date": "2022-04-13"
        },
        {
            "Cardio": 385,
            "Fat Burn": 106,
            "Light": 1173,
            "Peak": 37,
            "date": "2022-04-14"
        },
        {
            "Cardio": 368,
            "Fat Burn": 408,
            "Light": 1263,
            "Peak": 193,
            "date": "2022-04-15"
        },
        {
            "Cardio": 183,
            "Fat Burn": 140,
            "Light": 853,
            "Peak": 59,
            "date": "2022-04-16"
        },
        {
            "Cardio": 383,
            "Fat Burn": 139,
            "Light": 1062,
            "Peak": 125,
            "date": "2022-04-17"
        },
        {
            "Cardio": 309,
            "Fat Burn": 171,
            "Light": 1471,
            "Peak": 41,
            "date": "2022-04-18"
        },
        {
            "Cardio": 48,
            "Fat Burn": 213,
            "Light": 915,
            "Peak": 59,
            "date": "2022-04-19"
        },
        {
            "Cardio": 143,
            "Fat Burn": 248,
            "Light": 1279,
            "Peak": 176,
            "date": "2022-04-20"
        },
        {
            "Cardio": 28,
            "Fat Burn": 192,
            "Light": 553,
            "Peak": 178,
            "date": "2022-04-21"
        },
        {
            "Cardio": 317,
            "Fat Burn": 270,
            "Light": 1109,
            "Peak": 106,
            "date": "2022-04-22"
        },
        {
            "Cardio": 122,
            "Fat Burn": 100,
            "Light": 559,
            "Peak": 92,
            "date": "2022-04-23"
        },
        {
            "Cardio": 119,
            "Fat Burn": 107,
            "Light": 1294,
            "Peak": 131,
            "date": "2022-04-24"
                    }
                ],
                "result": "success"
        }

with open('./HeartRate.json', 'w') as outfile:
    json.dump(json_str, outfile)

# +


# Replace the below path with os.environ.get("SIHA_PATH") once the json is saved in the datafabric
siha_folder_path = './'
# siha_folder_path = os.environ.get("SIHA_PATH")

pipeline = SequenceOperator(
    [
        SihaDataset(siha_folder_path, table_name="HeartRate"),
        JqOperator(".data"),
        JsonNormalizeOperator(),
        ConvertToDatetimeOperator(
            feature_names=["date"], infer_datetime_format=True
        ),
        SetIndexOperator("date"),

    ]
)

df = pipeline.process()[0]
df

# +
# Month stats
df_agg_pipeline = AggregateOperator(
                      groupby_feature_names=[df.index.month], 
                      aggregation_definition= {'Cardio': ['sum', 'mean'],
                                               'Fat Burn': ['sum', 'mean'],
                                               'Light': ['sum', 'mean'],
                                               'Peak': ['mean']})



# Concatenate columns only
df_stats = df_agg_pipeline.process(df)[0]
df_stats

# +
model = LinearFitOperator(feature_names=['Cardio', 'Light', 'Peak'],
                          target='Fat Burn',
                          target_type='continuous')


results = model.process(df)[0]
print(f'model score: %f' % results[0])
print(f'model coefficients:', results[1])
print(f'model intercept:', results[2])
