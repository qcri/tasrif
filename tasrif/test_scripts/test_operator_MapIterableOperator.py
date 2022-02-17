# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import pandas as pd

from tasrif.processing_pipeline import MapIterableOperator
from tasrif.processing_pipeline.pandas import DropNAOperator

df1 = pd.DataFrame({
    'Date':   ['05-06-2021', '06-06-2021', '07-06-2021', '08-06-2021'],
    'Steps':  [        4500,         None,         5690,         6780]
 })

df2 = pd.DataFrame({
    'Date':   ['12-07-2021', '13-07-2021', '14-07-2021', '15-07-2021'],
    'Steps':  [        2100,         None,         None,         5400]
 })



operator = MapIterableOperator(DropNAOperator(axis=0))
result = operator.process([df1, df2])

result


# %%
def printdf(*df):
    for i in df:
        print(df)
    return *df


# %%
printdf(df1)

# %%
