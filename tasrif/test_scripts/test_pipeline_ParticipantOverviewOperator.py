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
import pandas as pd

from tasrif.processing_pipeline.custom import ParticipationOverviewOperator

# %% pycharm={"name": "#%%\n"}
df = pd.DataFrame( [
    ['2020-02-20', 1000, 1800, 1], ['2020-02-21', 5000, 2100, 1], ['2020-02-22', 10000, 2400, 1],
    ['2020-02-20', 0, 1600, 2], ['2020-02-21', 4000, 2000, 2], ['2020-02-22', 11000, 2400, 2],
    ['2020-02-20', 500, 2000, 3], ['2020-02-21', 0, 2700, 3], ['2020-02-22', 15000, 3100, 3]],
columns=['Day', 'Steps', 'Calories', 'PersonId'])

# %%
op = ParticipationOverviewOperator(participant_identifier='PersonId', date_feature_name='Day')

# %%
df1 = op.process(df)

# %%
df1

# %%
op2 = ParticipationOverviewOperator(participant_identifier='PersonId', date_feature_name='Day', overview_type='date_vs_features')

# %%
df2 = op2.process(df)

# %%
df2

# %%
# Count only days where the number of steps > 1000
ff = {
    'Steps': lambda x: x > 1000
}

op3 = ParticipationOverviewOperator(participant_identifier='PersonId', date_feature_name='Day', filter_features=ff)

# %%
df3 = op3.process(df)

# %%
df3

# %%
# Count only days where the number of steps > 1000

op4 = ParticipationOverviewOperator(participant_identifier='PersonId', date_feature_name='Day', overview_type='date_vs_features', filter_features=ff)

# %%
df4 = op4.process(df)

# %%
df4

# %%
df4[0].plot.bar(x='Day')

# %%
