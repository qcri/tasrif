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

from tasrif.processing_pipeline.pandas import FillNAOperator

df = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
                   "toy": [np.nan, 'Batmobile', 'Bullwhip'],
                   "born": [pd.Timestamp("1940-04-25"), pd.Timestamp("1940-04-25"),
                            pd.Timestamp("1940-04-25")]})

operator = FillNAOperator(axis=0, value='laptop')
df = operator.process(df)[0]
df
