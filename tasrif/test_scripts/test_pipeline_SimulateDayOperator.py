# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import numpy as np
import pandas as pd

from tasrif.processing_pipeline.pandas import RenameOperator
from tasrif.processing_pipeline.custom import SimulateDayOperator, AggregateOperator
# -

dates = pd.date_range("2016-12-31", "2020-01-03", freq="15T").to_series()
df = pd.DataFrame()
df["Date"] = dates
df["Steps"] = np.random.randint(0, 10, size=len(df))
df['ID'] = 'user1'

df

# +
operator = AggregateOperator(
    groupby_feature_names =["ID", df.index.hour],
    aggregation_definition= {"Steps": ["mean", "std"]}
)

df = operator.process(df)[0]

operator = RenameOperator(
    columns={'level_1': 'hour'}
)

df = operator.process(df)[0]
# -

df

# +
operator = SimulateDayOperator(
    sample_by=['hour', 'ID'],
    distribution_parameter_columns=['Steps_mean', 'Steps_std'],
    distribution_type='normal',
    samples=2,
    sample_column_name="sample",
    output_format='wide'
)

operator.process(df)[0]
