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

from tasrif.processing_pipeline.pandas import MergeOperator

# %%
# Full
df1 = pd.DataFrame({"id": [1, 2, 3], "colors": ["red", "white", "blue"]})
df2 = pd.DataFrame({"id": [1, 2, 3], "cities": ["Doha", "Vienna", "Belo Horizonte"]})

# %% pycharm={"name": "#%%\n"}
df1.head()

# %% pycharm={"name": "#%%\n"}
df2.head()

# %% pycharm={"name": "#%%\n"}
merged = MergeOperator().process(df1, df2)
merged
