# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import pandas as pd

from tasrif.processing_pipeline import MapProcessingOperator

df0 = pd.DataFrame(
    [
        [1, "2020-05-01 00:00:00", 1],
        [1, "2020-05-01 01:00:00", 1],
        [1, "2020-05-01 03:00:00", 2],
        [2, "2020-05-02 00:00:00", 1],
        [2, "2020-05-02 01:00:00", 1],
    ],
    columns=["logId", "timestamp", "sleep_level"],
)

df1 = pd.DataFrame(
    [["tom", 10], ["Alfred", 15], ["Alfred", 18], ["juli", 14]], columns=["name", "age"]
)


class SizeOperator(MapProcessingOperator):
    def _processing_function(self, df):
        return df.size


SizeOperator(num_processes=1).process(df0, df1)
