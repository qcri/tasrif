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

import numpy as np

# %%
import pandas as pd

from tasrif.processing_pipeline.observers import DataprepObserver
from tasrif.processing_pipeline.pandas import RenameOperator

# %%
# Prepare two days data
two_days = 48 * 2
idx = pd.date_range("2018-01-01", periods=two_days, freq="30T", name="startTime")
activity = np.random.randint(0, 100, two_days)
df = pd.DataFrame(data=activity, index=idx, columns=["activity"])
df["steps"] = np.random.randint(100, 1000, two_days)
df["sleep"] = False

# reduce activity between 23:30 - 08:00
time_filter = df.between_time(start_time="23:30", end_time="8:00").index
df.loc[time_filter, "sleep"] = True
df.loc[time_filter, "activity"] = df.loc[time_filter, "activity"] / 100
df.loc[time_filter, "steps"] = 0
df

# %%
df = RenameOperator(
    columns={"logId": "id"}, observers=[DataprepObserver(method="distribution,missing")]
).process(df)
