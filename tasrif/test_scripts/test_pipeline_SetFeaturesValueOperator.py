# ---
# jupyter:
#   jupytext:
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
import numpy as np

from tasrif.processing_pipeline.custom import SetFeaturesValueOperator

df0 = pd.DataFrame([['Tom', 10], ['Alfred', 15], ['Alfred', 18], ['Juli', 14]], columns=['name', 'score'])
df1 = pd.DataFrame({"name": ['Alfred', 'juli', 'Tom', 'Ali'],
                   "score": [np.nan, 155, 159, 165],
                   "born": [pd.NaT, pd.Timestamp("2010-04-25"), pd.NaT,
                            pd.NaT]})

print(df0)
print(df1)

print()
print('=================================================')
print('select rows where score >= 13')
operator = SetFeaturesValueOperator(selector=lambda df: df.score >= 13)
print(operator.process(df0, df1))

print()
print('=================================================')
print('select rows where score >= 13 and set their scores to 15')
operator = SetFeaturesValueOperator(selector=lambda df: df.score >= 13, 
                                    feature_names=['score'],
                                    value=15)
df0, df1 = operator.process(df0, df1)
print(df0)
print(df1)


