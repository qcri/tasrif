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

from tasrif.processing_pipeline.pandas import SortOperator

# %%
df1 = pd.DataFrame({'id': [1, 2, 3], 'colors': ['red', 'white', 'blue'], "importance": [1, 3, 2]})
df2 = pd.DataFrame({'id': [1, 2, 3], 'cities': ['Doha', 'Vienna', 'Belo Horizonte'], "importance": [3, 2, 1]})

# %% pycharm={"name": "#%%\n"}
sorted = SortOperator(by="importance").process(df1, df2)

# %% pycharm={"name": "#%%\n"}
sorted[0]

# %% pycharm={"name": "#%%\n"}
sorted[1]
