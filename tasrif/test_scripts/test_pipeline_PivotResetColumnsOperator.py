# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import pandas as pd
from tasrif.processing_pipeline.pandas import PivotResetColumnsOperator
df = pd.DataFrame([
    [1, "2020-05-01 00:00:00", 1],
    [1, "2020-05-01 01:00:00", 1],
    [1, "2020-05-01 03:00:00", 2],
    [2, "2020-05-02 00:00:00", 1],
    [2, "2020-05-02 01:00:00", 1]],
    columns=['logId', 'timestamp', 'sleep_level'])

df['timestamp'] = pd.to_datetime(df['timestamp'])
op = PivotResetColumnsOperator(level=0, index='timestamp', columns='logId', values='sleep_level')
op.process(df)[0]
