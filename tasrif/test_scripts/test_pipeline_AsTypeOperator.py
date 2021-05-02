# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
from tasrif.processing_pipeline.pandas import AsTypeOperator
df = pd.DataFrame([
    [1, "2020-05-01 00:00:00", "1", "3"],
    [1, "2020-05-01 01:00:00", "1", "5" ], 
    [1, "2020-05-01 03:00:00", "2", "3"], 
    [2, "2020-05-02 00:00:00", "1", "10"],
    [2, "2020-05-02 01:00:00", "1", "0"]],
    columns=['logId', 'timestamp', 'sleep_level', 'awake_count'])

df.dtypes

# %%
op = AsTypeOperator({'sleep_level': 'int32', 'awake_count' : 'float32'})
df1 = op.process(df)
df1[0].dtypes
