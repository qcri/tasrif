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

import datetime

import numpy as np

# +
import pandas as pd

from tasrif.processing_pipeline.custom import FilterOperator

df = pd.DataFrame({
        'Hours': pd.date_range('2018-01-01', '2018-01-10', freq='1H', closed='left'),
        'Steps': np.random.randint(100,10000, size=9*24),
        }
     )

ids = []
for i in range(1, 217):
    ids.append(i%10 + 1)
    
df["Id"] = ids


# Add day for id 1
df = df.append({'Hours': datetime.datetime(2020, 2, 2), 'Steps': 2000, 'Id': 1}, ignore_index=True)

# Remove 5 days from id 10
id_10_indices = df.loc[df.Id == 10].index.values[:-5]
df = df[~df.index.isin(id_10_indices)]
# -

operator = FilterOperator(participant_identifier="Id",
                          date_feature_name="Hours",
                          epoch_filter=lambda df: df['Steps'] > 10,
                          day_filter={
                              "column": "Hours",
                              "filter": lambda x: x.count() < 10,
                              "consecutive_days": (7, 12) # 7 minimum consecutive days, and 12 max
                          },
                          filter_type="include")
operator.process(df)[0]

df = pd.DataFrame([
    [1, "2020-05-01 00:00:00", "1", "3"],
    [1, "2020-05-01 01:00:00", "1", "5" ],
    [2, "2020-05-01 03:00:00", "2", "3"],
    [2, "2020-05-02 00:00:00", "1", "10"],
    [3, "2020-05-02 01:00:00", "1", "0"],
    [4, "2020-05-03 01:00:00", "1", "0"]],
    columns=['logId', 'timestamp', 'sleep_level', 'awake_count'])
op = FilterOperator(participant_identifier="logId",
                    participant_filter=[1, 3],
                    filter_type="include",)
df1 = op.process(df)
df1[0]

