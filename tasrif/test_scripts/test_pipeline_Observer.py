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
from tasrif.processing_pipeline.observers import FunctionalObserver, LoggingObserver
from tasrif.processing_pipeline.pandas import ReplaceOperator

# %%
# Full
df = pd.DataFrame(
    {"id": [1, 2, 3], "colors": ["red", "white", "blue"], "importance": [1, 3, 2]}
)


# %% pycharm={"name": "#%%\n"}
class PrintHead(Observer):
    def _observe(self, operator, dfs):
        for df in dfs:
            print(df.head())


class PrintFirstRow(FunctionalObserver):
    def _observe(self, operator, dfs):
        for df in dfs:
            print(df.iloc[0])


# %%
df_replaced = ReplaceOperator(
    to_replace="sleep_level", value="sleep", observers=[PrintHead()]
).process(df)

df_replaced

# %% pycharm={"name": "#%%\n"}
pipeline = SequenceOperator(
    [
        ReplaceOperator(to_replace="green", value="red"),
        ReplaceOperator(to_replace="red", value="green", observers=[PrintHead()]),
    ],
    observers=[PrintFirstRow()],
)
result = pipeline.process(df)
result
