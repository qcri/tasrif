# ---
# jupyter:
#   jupytext:
#     formats: py:percent
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

from tasrif.processing_pipeline.custom import OneHotEncoderOperator

# %%
# Full
df = pd.DataFrame(
    {
        "id": [1, 2, 3],
        "colors": ["red", "white", "blue"],
        "cities": ["Doha", "Vienna", "Belo Horizonte"],
        "multiple": ["1,2", "1", "1,3"],
    }
)
df

# %% pycharm={"name": "#%%\n"}
OneHotEncoderOperator(feature_names=["colors"], drop_first=True).process(df)[0]

# %% pycharm={"name": "#%%\n"}
OneHotEncoderOperator(feature_names=["colors"], drop_first=False).process(df)[0]

# %%
OneHotEncoderOperator(feature_names=["colors", "multiple"], drop_first=False).process(
    df
)[0]
