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

from tasrif.processing_pipeline.custom import JsonPivotOperator

# %%
df = pd.DataFrame({'id': [1, 2, 3], 'data':["{\"calories\":1000, \"distance\":5, \"steps\":2}", "{\"calories\":2000, \"distance\":15, \"steps\":12}", "{\"calories\":1000, \"distance\":5, \"steps\":2}"]})
df

# %% pycharm={"name": "#%%\n"}
op = JsonPivotOperator(['data'])
op.process(df)

op
