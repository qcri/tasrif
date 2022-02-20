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
# %load_ext autoreload
# %autoreload 2


# %% pycharm={"name": "#%%\n"}
import pandas as pd

from tasrif.processing_pipeline.custom import StatisticsOperator

df = pd.DataFrame(
    [
        ["2020-02-20", 1000, 1800, 1],
        ["2020-02-21", 5000, 2100, 1],
        ["2020-02-22", 10000, 2400, 1],
        ["2020-02-20", 1000, 1800, 1],
        ["2020-02-21", 5000, 2100, 1],
        ["2020-02-22", 10000, 2400, 1],
        ["2020-02-20", 0, 1600, 2],
        ["2020-02-21", 4000, 2000, 2],
        ["2020-02-22", 11000, 2400, 2],
        ["2020-02-20", None, 2000, 3],
        ["2020-02-21", 0, 2700, 3],
        ["2020-02-22", 15000, 3100, 3],
    ],
    columns=["Day", "Steps", "Calories", "PersonId"],
)

# %%
filter_features = {"Steps": lambda x: x > 0}

sop = StatisticsOperator(
    participant_identifier="PersonId",
    date_feature_name="Day",
    filter_features=filter_features,
)
sop.process(df)
