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

import numpy as np

# %%
import pandas as pd

from tasrif.processing_pipeline.pandas import ResetIndexOperator

df = pd.DataFrame([
    [1,'2016-03-12 01:00:00',10],
    [1,'2016-03-12 04:00:00',250],
    [1,'2016-03-12 06:00:00',30],
    [1,'2016-03-12 20:00:00',10],
    [1,'2016-03-12 23:00:00',23],
    [2,'2016-03-12 00:05:00',20],
    [2,'2016-03-12 19:06:00',120],
    [2,'2016-03-12 21:07:00',100],
    [2,'2016-03-12 23:08:00',50],
    [3,'2016-03-12 10:00:00',300]
], columns=['Id', 'ActivityTime', 'Calories'])

df['ActivityTime'] = pd.to_datetime(df['ActivityTime'])


df = df.set_index('ActivityTime')
operator = ResetIndexOperator()
df = operator.process(df)[0]
df
