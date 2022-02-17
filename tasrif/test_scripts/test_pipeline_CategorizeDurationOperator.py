# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% tags=[]
# %load_ext autoreload
# %autoreload 2

from datetime import timedelta

import numpy as np
import pandas as pd
import seaborn as sns

from tasrif.processing_pipeline.custom import CategorizeDurationOperator

dates = pd.date_range('2016-12-31', '2017-01-08', freq='D').to_series()
df = pd.DataFrame()
df["Date"] = dates
df['Last_Date'] = df['Date'].apply(lambda x: x + timedelta(days=np.random.randint(3), 
                                                           hours=np.random.randint(24), 
                                                           minutes=np.random.randint(60)))
df['Duration'] = df['Last_Date'] - df['Date']
df['Steps'] = np.random.randint(1000,25000, size=len(df))
df['Calories'] = np.random.randint(1800,3000, size=len(df))

# %%
df # pylint: disable=pointless-statement

# %%
df1 = df.copy()
operator = CategorizeDurationOperator(duration_feature_name="Duration", category_definition="day")
df1 = operator.process(df1)[0]
df1 # pylint: disable=pointless-statement

# %%
df3 = df.copy()
operator = CategorizeDurationOperator(duration_feature_name="Duration", category_definition=["day", "hour"])
df3 = operator.process(df3)[0]
df3 # pylint: disable=pointless-statement

# %%
df4 = df.copy()
operator = CategorizeDurationOperator(
    duration_feature_name="Duration", category_definition=[{"day": "days_difference"}, {"hour": "hours_difference"}])
df4 = operator.process(df4)[0]
df4 # pylint: disable=pointless-statement
