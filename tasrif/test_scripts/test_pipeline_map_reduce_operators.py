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
import numpy as np

from tasrif.processing_pipeline.pandas import DropNAOperator, ConcatOperator
from tasrif.processing_pipeline import ProcessingPipeline, ReduceOperator, MapOperator


df1 = pd.DataFrame([
    [1,'2016-03-12 01:00:00',10],
    [1,'2016-03-12 04:00:00',250],
    [1,'2016-03-12 06:00:00',30],
    [1,'2016-03-12 20:00:00',10],
    [1,'2016-03-12 21:00:00',np.NaN],
    [1,'2016-03-12 23:00:00',23]], columns=['Id', 'ActivityTime', 'Calories'])
df2 = pd.DataFrame([
    [2,'2016-03-12 00:05:00',20],
    [2,'2016-03-12 00:06:00',np.NaN],
    [2,'2016-03-12 19:06:00',120],
    [2,'2016-03-12 21:07:00',100],
    [2,'2016-03-12 23:08:00',50]], columns=['Id', 'ActivityTime', 'Calories'])
df3 = pd.DataFrame([
    [3,'2016-03-12 10:00:00',np.NaN],
    [3,'2016-03-12 10:00:00',300]], columns=['Id', 'ActivityTime', 'Calories'])

dfs = []
dfs.append(df1)
dfs.append(df2)
dfs.append(df3)




# %%
pipeline = ProcessingPipeline([
            MapOperator(DropNAOperator()),
            ReduceOperator(ConcatOperator())])

pipeline.process(*dfs)

# %%


