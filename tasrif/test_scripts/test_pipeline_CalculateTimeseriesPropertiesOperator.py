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

from tasrif.processing_pipeline.kats import CalculateTimeseriesPropertiesOperator

dates = pd.date_range('2016-12-31', '2020-01-08', freq='D').to_series()
df = pd.DataFrame()
df["Date"] = dates
df['Steps'] = np.random.randint(1000,25000, size=len(df))
df['Calories'] = np.random.randint(1800,3000, size=len(df))
# -

df

operator = CalculateTimeseriesPropertiesOperator(date_feature_name="Date", value_column='Steps')
features = operator.process(df)[0]
features
