"""Example on how to read sleep data from SIHA
"""
import oss

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
siha_folder_path = os.environ.get("SIHA_PATH")

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
