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
from tasrif.processing_pipeline import SequenceOperator, Observer

# %%
# Full
df = pd.DataFrame({'id': [1, 2, 3], 'colors': ['red', 'white', 'blue'], "importance": [1, 3, 2]})


# %% pycharm={"name": "#%%\n"}
class PrintHead(Observer):
    def observe(operator, dfs):
        for df in dfs:
            print(df.head())
            
class PrintFirstRow(Observer):
    def observe(operator, dfs):
        for df in dfs:
            print(df.iloc[0]) 


# %%
df = ReplaceOperator(to_replace="red", value="green", observers=[PrintHead]).process(df)[0]

df

# %% pycharm={"name": "#%%\n"}
pipeline = SequenceOperator([ReplaceOperator(to_replace="green", value="red", observers=[PrintFirstRow]), ReplaceOperator(to_replace="red", value="green", observers=[PrintFirstRow, PrintHead])], observers=[PrintHead])
result = pipeline.process(df)


