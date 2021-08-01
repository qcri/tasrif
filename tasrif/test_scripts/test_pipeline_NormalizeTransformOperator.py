# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
from sklearn.model_selection import train_test_split
from tasrif.processing_pipeline.custom import NormalizeOperator
from tasrif.processing_pipeline.custom import NormalizeTransformOperator

df = pd.DataFrame([
    [1, "2020-05-01 00:00:00", 10],
    [1, "2020-05-01 01:00:00", 15], 
    [1, "2020-05-01 03:00:00", 23], 
    [2, "2020-05-02 00:00:00", 17],
    [2, "2020-05-02 01:00:00", 11]],
    columns=['logId', 'timestamp', 'sleep_level'])

X_train, X_test, y_train, y_test = train_test_split(df['timestamp'], df['sleep_level'], test_size=0.4)

print(y_train.to_frame())

op1 = NormalizeOperator('all', 'minmax', {'feature_range': (0, 2)})

output1 = op1.process(y_train.to_frame())

print(output1)

processed_train_y = output1[0][0]
trained_model = output1[0][1]

op2 = NormalizeTransformOperator('all', trained_model)

output2 = op2.process(y_test.to_frame())

print(output2)
