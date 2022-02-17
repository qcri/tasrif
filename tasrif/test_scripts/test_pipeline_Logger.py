# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2
import pandas as pd

from tasrif.processing_pipeline import Observer, SequenceOperator
from tasrif.processing_pipeline.observers import (
    FunctionalObserver,
    GroupbyLogger,
    Logger,
)
from tasrif.processing_pipeline.pandas import RenameOperator

# %%
df = pd.DataFrame([
    [1, "2020-05-01 00:00:00", 1],
    [1, "2020-05-01 01:00:00", 1], 
    [1, "2020-05-01 03:00:00", 2], 
    [2, "2020-05-02 00:00:00", 1],
    [2, "2020-05-02 01:00:00", 1]],
    columns=['logId', 'timestamp', 'sleep_level'])

# %%
df = RenameOperator(columns={"logId": "id"}, observers=[Logger()]).process(df)

df = df[0]

# %%
df = RenameOperator(columns={"sleep_level": "sleep"}, observers=[GroupbyLogger('id', method="first,last")]).process(df)

# %% pycharm={"name": "#%%\n"}
pipeline = SequenceOperator([RenameOperator(columns={"timestamp": "time"}), RenameOperator(columns={"time": "time_difference"})], observers=[Logger("head,tail")])
result = pipeline.process(df[0])
result
