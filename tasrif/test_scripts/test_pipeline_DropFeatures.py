# ---
# jupyter:
#   jupytext:
#     formats: py:percent
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
import numpy as np

# import tasrif.processing_pipeline
from tasrif.processing_pipeline.pandas import DropFeaturesOperator

df0 = pd.DataFrame([['tom', 10], ['nick', 15], ['juli', 14]], columns=['name', 'age'])
df1 = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
                  "toy": [np.nan, 'Batmobile', 'Bullwhip'],
                  "born": [pd.NaT, pd.Timestamp("1940-04-25"),
                           pd.NaT]})

print(df0)
print(df1)

operator = DropFeaturesOperator(drop_features=[])
df0, df1 = operator.process(df0, df1)

print(df0)
print(df1)
