# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: PyCharm (tasrif)
#     language: python
#     name: pycharm-5bd30262
# ---

# %%
# %load_ext autoreload
# %autoreload 2
import os
import numpy as np

from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SortOperator, ReplaceOperator
from tasrif.processing_pipeline.pandas import DropDuplicatesOperator, DropNAOperator, DropFeaturesOperator
from tasrif.processing_pipeline.custom import OneHotEncoderOperator

from tasrif.data_readers.sleep_health import SleepAssessmentDataset


# %%
def col_stats(df):
    print('Some important stats:')
    print('\t- This dataset contains', len(df) ,' rows.')
    for col in df.columns:
        null_percentage = "{:.2f}".format(df[col].isnull().sum()/len(df)*100)
        print('\t - ``', col, '`` has', df[col].isnull().sum(), 'NAs (', df[col].count().sum(), '/', len(df), ') =',
              null_percentage, '%')


# %% pycharm={"name": "#%%\n"}
# Full MyFamilyDataset
mf = SleepAssessmentDataset(os.environ['SLEEPHEALTH_SLEEPASSESSMENT_PATH'], pipeline=None)
df = mf.raw_df.copy()
print("Shape:", df.shape)
df.head()

# %% pycharm={"name": "#%%\n"}
col_stats(df)

# %% pycharm={"name": "#%%\n"}
print("Shape after dropping duplicate participants:", df["participantId"].drop_duplicates().shape)

# %% pycharm={"name": "#%%\n"}
# Default Pipeline
pipeline = SequenceOperator([
    ConvertToDatetimeOperator(feature_names="timestamp", format="%Y-%m-%dT%H:%M:%S%z", utc=True),
    SortOperator(by=["participantId", "timestamp"]),
    DropDuplicatesOperator(subset="participantId", keep="last"),
    ReplaceOperator(to_replace={"alcohol": {7: np.nan},
                                "medication_by_doctor": {7: np.nan},
                                "sleep_aids": {7: np.nan},
                                "told_by_doctor": {3: np.nan},
                                "told_to_doctor": {3: np.nan},
                                "told_by_doctor_specify": {np.nan: '8'},
                                "other_selected": {np.nan: ''},
                                }),
    DropNAOperator(subset=['alcohol', 'concentrating_problem_one', 'concentrating_problem_two',
                           'discomfort_in_sleep', 'exercise', 'fatigue_limit', 'feel_tired_frequency',
                           'felt_alert', 'had_problem', 'hard_times', 'medication_by_doctor',
                           'poor_sleep_problems', 'sleep_aids', 'sleep_problem', 'think_clearly',
                           'tired_easily', 'told_by_doctor',  'told_to_doctor', 'trouble_staying_awake']),
    OneHotEncoderOperator(feature_names=['alcohol', 'concentrating_problem_one', 'concentrating_problem_two',
                           'discomfort_in_sleep', 'exercise', 'fatigue_limit', 'feel_tired_frequency',
                           'felt_alert', 'had_problem', 'hard_times', 'medication_by_doctor',
                           'poor_sleep_problems', 'sleep_aids', 'sleep_problem', 'think_clearly',
                           'tired_easily', 'told_by_doctor',  'told_to_doctor', 'trouble_staying_awake',
                            'told_by_doctor_specify'],
                            drop_first=True),
    ])


mypipe = SleepAssessmentDataset(os.environ['SLEEPHEALTH_SLEEPASSESSMENT_PATH'], pipeline=pipeline)
df_piped = mypipe.processed_dataframe()
print("Shape:", df_piped.shape)
df_piped.head()

# %% pycharm={"name": "#%%\n"}
df_piped["other_selected"].unique()

# %% pycharm={"name": "#%%\n"}
