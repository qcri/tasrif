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
from tasrif.processing_pipeline ProcessDataframeOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator

df0 = pd.DataFrame([[1, "2020-05-01 00:00:00", 1], [1, "2020-05-01 01:00:00", 1],
                    [1, "2020-05-01 03:00:00", 2], [2, "2020-05-02 00:00:00", 1],
                    [2, "2020-05-02 01:00:00", 1]],
                    columns=['logId', 'timestamp', 'sleep_level'])

df1 = pd.DataFrame([['tom', 10],
                    ['Alfred', 15],
                    ['Alfred', 18],
                    ['juli', 14]],
                    columns=['name', 'age'])

df2 = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
                    "toy": [None, 'Batmobile', 'Bullwhip'],
                    "age": [38, 25, 23]})

# %%
# Process df1, df2, but not df0
#
# Shortcut of doing
# ComposeOperator([
#     NoopOperator(),
#     feature_creator,
#     feature_creator,
# ])

feature_creator = CreateFeatureOperator("name_age", lambda df: df["name"] + "_" + str(df["age"]))

ProcessDataframeOperator(index=[1, 2], processing_operators=[feature_creator]).process(df0, df1, df2)
