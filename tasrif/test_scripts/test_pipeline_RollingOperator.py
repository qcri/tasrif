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
from tasrif.processing_pipeline.pandas import RollingOperator

df = pd.DataFrame({'B': [0, 1, 2, 3, 4]})
op = RollingOperator(2)
op.process(df)[0].sum()
