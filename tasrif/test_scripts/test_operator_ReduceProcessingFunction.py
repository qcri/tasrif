# ---
# jupyter:
#   jupytext:
#     formats: py:percent
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
import pandas as pd
from tasrif.processing_pipeline import ReduceProcessingOperator

df0 = pd.DataFrame([[1, "2020-05-01 00:00:00", 1], [1, "2020-05-01 01:00:00", 1],
                    [1, "2020-05-01 03:00:00", 2], [2, "2020-05-02 00:00:00", 1],
                    [2, "2020-05-02 01:00:00", 1]],
                    columns=['logId', 'timestamp', 'sleep_level'])

df1 = pd.DataFrame([['tom', 10],
                    ['Alfred', 15],
                    ['Alfred', 18],
                    ['juli', 14]],
                    columns=['name', 'age'])

class AppendOperator(ReduceProcessingOperator):
    initial = pd.DataFrame([["Harry", "2020-05-01 00:00:00"]],
                            columns=["name", "timestamp"])

    def _processing_function(self, df_to_append, dfs):
        return dfs.append(df_to_append)

AppendOperator().process(df0, df1)
