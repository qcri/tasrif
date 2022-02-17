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
# %load_ext autoreload
# %autoreload 2
import pandas as pd

from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator

# %%
# Full
df0 = pd.DataFrame([[1, "2020-05-01 00:00:00", 1], [1, "2020-05-01 01:00:00", 1],
                    [1, "2020-05-01 03:00:00", 2], [2, "2020-05-02 00:00:00", 1],
                    [2, "2020-05-02 01:00:00", 1]],
                    columns=['logId', 'timestamp', 'sleep_level'])

df0

# %% pycharm={"name": "#%%\n"}
df0["timestamp"].dtype

# %% pycharm={"name": "#%%\n"}
df0 = ConvertToDatetimeOperator(feature_names=["timestamp"], utc=True).process(df0)[0]
df0

# %% pycharm={"name": "#%%\n"}
df0["timestamp"].dtype

