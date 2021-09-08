# ---
# jupyter:
#   jupytext:
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

from tasrif.processing_pipeline.custom import SetFeaturesValueOperator

df0 = pd.DataFrame([['tom', 10], ['nick', 15], ['juli', 14]])
df0.columns = ['name', 'age']
df1 = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
                   "toy": [np.nan, 'Batmobile', 'Bullwhip'],
                   "age": [11, 14, 17]})

print(df0)
print(df1)

print()
print('=================================================')
print('select rows where age >= 13')
operator = SetFeaturesValueOperator(selector=lambda df: df.age >= 13)
print(operator.process(df0, df1))

print()
print('=================================================')
print('select rows where age >= 13 and set their ages to 15')
operator = SetFeaturesValueOperator(selector=lambda df: df.age >= 13, 
                                    feature_names=['age'],
                                    value=15)
df0, df1 = operator.process(df0, df1)
print(df0)
print(df1)


