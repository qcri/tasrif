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
from tasrif.processing_pipeline.pandas import ReplaceOperator

# %%
# Full
df = pd.DataFrame({'id': [1, 2, 3], 'colors': ['red', 'white', 'blue'], "importance": [1, 3, 2]})


# %% pycharm={"name": "#%%\n"}

df = ReplaceOperator(to_replace="red", value="green").process(df)[0]

# %% pycharm={"name": "#%%\n"}
df


