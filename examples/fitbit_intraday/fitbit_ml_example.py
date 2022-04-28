# +
import os
import numpy as np
import pandas as pd

from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConvertToDatetimeOperator,
    JsonNormalizeOperator,
    SetIndexOperator,
    RenameOperator,
    ResetIndexOperator,
    DropFeaturesOperator,
)

from tasrif.processing_pipeline.custom import (
    SimulateDayOperator,
    AggregateOperator,
    CreateFeatureOperator
)

from tasrif.processing_pipeline.tsfresh import TSFreshFeatureExtractorOperator
# -

# Load the data
dates = pd.date_range("2016-12-31", "2020-01-03", freq="15T").to_series()
df = pd.DataFrame()
df["Date"] = dates.values
df["Steps"] = np.random.randint(0, 10, size=len(df))
df['ID'] = 'user1'
df

# +
# Aggregate data
pipeline = SequenceOperator([
    AggregateOperator(
        groupby_feature_names =["ID", df['Date'].dt.time],
        aggregation_definition= {"Steps": ["mean", "std"]}
    ),
    RenameOperator(columns={'Date': 'time'})    
])

aggregate = pipeline.process(df)[0]
aggregate

# +
# Simulate day given parameters
pipeline = SequenceOperator([
    SimulateDayOperator(
        sample_by=['time', 'ID'],
        distribution_parameter_columns=['Steps_mean', 'Steps_std'],
        distribution_type='normal',
        samples=100,
        sample_column_name="sample",
        output_format='long'
    ),
    ResetIndexOperator(),
    CreateFeatureOperator(
        feature_name='ID', 
        feature_creator=lambda df: df['ID'] + '_' + str(df['sample'])
    ),
    DropFeaturesOperator(feature_names=['sample'])
])

samples = pipeline.process(aggregate)[0]
samples

# +
# Extract features
operator = TSFreshFeatureExtractorOperator(
    seq_id_col="ID", 
    date_feature_name='time',
    value_col='sample_value'
)

features = operator.process(samples)[0]
features
