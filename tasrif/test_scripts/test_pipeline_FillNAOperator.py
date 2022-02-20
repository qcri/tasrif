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

import numpy as np

# %%
import pandas as pd

from tasrif.processing_pipeline.pandas import FillNAOperator

df = pd.DataFrame(
    {
        "name": ["Alfred", "juli", "Tom", "Ali"],
        "height": [np.nan, 155, 159, 165],
        "born": [pd.NaT, pd.Timestamp("2010-04-25"), pd.NaT, pd.NaT],
    }
)

operator = FillNAOperator(axis=0, value="laptop")
df = operator.process(df)[0]
df
