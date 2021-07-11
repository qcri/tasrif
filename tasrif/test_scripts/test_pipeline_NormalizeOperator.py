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
from tasrif.processing_pipeline.custom import NormalizeOperator
df = pd.DataFrame([
    [1, "2020-05-01 00:00:00", 10],
    [1, "2020-05-01 01:00:00", 15], 
    [1, "2020-05-01 03:00:00", 23], 
    [2, "2020-05-02 00:00:00", 17],
    [2, "2020-05-02 01:00:00", 11]],
    columns=['logId', 'timestamp', 'sleep_level'])

op = NormalizeOperator('all', 'minmax', {'feature_range': (0, 2)})
op.process(df)
