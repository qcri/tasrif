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

from tasrif.processing_pipeline.custom import DistributedUpsampleOperator

df = pd.DataFrame(
    [["2020-05-01", 16.5], ["2020-05-02", 19.1], ["2020-05-03", 0]],
    columns=["timestamp", "sedentary_hours"],
)

df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.set_index("timestamp")
op = DistributedUpsampleOperator("6h")
df = op.process(df)

# %%
df
