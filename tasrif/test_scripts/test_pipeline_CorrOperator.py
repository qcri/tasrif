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

from tasrif.processing_pipeline.pandas import CorrOperator

df = pd.DataFrame([
    [1, 1, 3],
    [1, 1, 5],
    [1, 2, 3],
    [2, 1, 10],
    [2, 1, 0]],
    columns=['logId', 'sleep_level', 'awake_count'])

df = df.set_index('logId')
op = CorrOperator()
df1 = op.process(df)
df1[0]
