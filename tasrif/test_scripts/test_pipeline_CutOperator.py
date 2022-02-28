# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import datetime

import numpy as np

# +
import pandas as pd

from tasrif.processing_pipeline.pandas import CutOperator

df = pd.DataFrame(
    {
        "Time": pd.date_range("2018-01-01", "2018-01-10", freq="1H", closed="left"),
        "Steps": np.random.randint(100, 5000, size=9 * 24),
    }
)

ids = []
for i in range(1, 217):
    ids.append(i % 10 + 1)

df["Id"] = ids
# -

df

# +
# 4 Equal width bins
df1 = df.copy()
operator = CutOperator(
    cut_column_name="Steps", bin_column_name="Bin", bins=4, retbins=True
)

df1, bins = operator.process(df1)[0]
print("Bins:", bins)
df1

# +
# Custom bins
cut_labels = ["Sedentary", "Light", "Moderate", "Vigorous"]
cut_bins = [0, 500, 2000, 6000, float("inf")]

df2 = df.copy()
operator = CutOperator(
    cut_column_name="Steps", bin_column_name="Bin", bins=cut_bins, labels=cut_labels
)

df2 = operator.process(df1)[0]
print(df2["Bin"].value_counts())
df2
