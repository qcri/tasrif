# ---
# jupyter:
#   jupytext:
#     formats: py:percent
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

# %%
import pandas as pd

from tasrif.processing_pipeline.custom import LinearFitOperator

df = pd.DataFrame(
    [
        [1, "2020-05-01 00:00:00", 10, "poor"],
        [1, "2020-05-01 01:00:00", 15, "poor"],
        [1, "2020-05-01 03:00:00", 23, "good"],
        [2, "2020-05-02 00:00:00", 17, "good"],
        [2, "2020-05-02 01:00:00", 11, "poor"],
    ],
    columns=["logId", "timestamp", "sleep_level", "sleep_quality"],
)

op = LinearFitOperator(
    feature_names="sleep_level", target="sleep_quality", target_type="categorical"
)
print(op.process(df))

df = pd.DataFrame(
    [
        [15, 10, "poor"],
        [13, 15, "poor"],
        [11, 23, "good"],
        [25, 17, "good"],
        [20, 11, "poor"],
    ],
    columns=["feature1", "feature2", "target"],
)

op = LinearFitOperator(feature_names="all", target="target", target_type="categorical")
op.process(df)
