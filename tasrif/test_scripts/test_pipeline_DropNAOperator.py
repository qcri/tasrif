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

import numpy as np

# %%
import pandas as pd

from tasrif.processing_pipeline.pandas import DropNAOperator

df0 = pd.DataFrame([['Tom', 10], ['Alfred', 15], ['Alfred', 18], ['Juli', 14]], columns=['name', 'score'])
df1 = pd.DataFrame({"name": ['Alfred', 'juli', 'Tom', 'Ali'],
                   "height": [np.nan, 155, 159, 165],
                   "born": [pd.NaT, pd.Timestamp("2010-04-25"), pd.NaT,
                            pd.NaT]})


operator = DropNAOperator(axis=0)
df0, df1 = operator.process(df0, df1)

print(df0)
print(df1)
