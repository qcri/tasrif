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

from tasrif.processing_pipeline.pandas import DropNAOperator

df0 = pd.DataFrame([['tom', 10], ['nick', 15], ['juli', 14]])
df1 = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
                   "toy": [np.nan, 'Batmobile', 'Bullwhip'],
                   "born": [pd.NaT, pd.Timestamp("1940-04-25"),
                            pd.NaT]})

operator = DropNAOperator(axis=0)
df0, df1 = operator.process(df0, df1)

print(df0)
print(df1)
