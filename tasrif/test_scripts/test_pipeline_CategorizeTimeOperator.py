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
import numpy as np
import pandas as pd
import seaborn as sns
from tasrif.processing_pipeline.custom import CategorizeTimeOperator


dates = pd.date_range('2016-12-31', '2017-01-08', freq='D').to_series()
df = pd.DataFrame()
df["Date"] = dates
df['Steps'] = np.random.randint(1000,25000, size=len(df))
df['Calories'] = np.random.randint(1800,3000, size=len(df))

# %%
df # pylint: disable=pointless-statement

# %%
df1 = df.copy()
operator = CategorizeTimeOperator(date_feature_name="Date", category_definition="day")
df1 = operator.process(df1)[0]
df1 # pylint: disable=pointless-statement

# %%
df2 = df.copy()
operator = CategorizeTimeOperator(date_feature_name="Date", category_definition="month")
df2 = operator.process(df2)[0]
df2 # pylint: disable=pointless-statement

# %%
df3 = df.copy()
operator = CategorizeTimeOperator(date_feature_name="Date", category_definition=["day", "month"])
df3 = operator.process(df3)[0]
df3 # pylint: disable=pointless-statement

# %%
df4 = df.copy()
operator = CategorizeTimeOperator(
    date_feature_name="Date", category_definition=[{"day": "day_of_week"}, {"month": "calendar_month"}])
df4 = operator.process(df4)[0]
df4 # pylint: disable=pointless-statement

# %%
df5 = df.copy()
operator = CategorizeTimeOperator(
    date_feature_name="Date",
    category_definition=[
        {"day": "weekday", "values": [1, 1, 1, 1, 0, 0, 1]},
        {"month": "in_may", "values": [0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]}])
df5 = operator.process(df5)[0]
df5 # pylint: disable=pointless-statement

# %%
sns.boxplot(y="Steps", x="weekday", data=df5)
