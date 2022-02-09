# +
import os
import pandas as pd
import numpy as np
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import math

from sklearn.model_selection import train_test_split 
from datetime import timedelta, datetime
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import RenameOperator
from tasrif.processing_pipeline.custom import AggregateActivityDatesOperator, CreateFeatureOperator


df = pd.read_csv("./minuteSleep_merged_2.csv")
# -

df


# +
def calculate_total_duration(df):
    return (df['Asleep'] + df['Restless'] + df['Awake'])

def calculate_sleep_quality(df):
    return df['Asleep']/(df['Asleep'] + df['Restless'] + df['Awake'])

pipeline = SequenceOperator([AggregateActivityDatesOperator(date_feature_name="date",
                                          participant_identifier=['Id', 'logId'],
                                          aggregation_definition={'value': [lambda x: (x[x == 1] == 1).sum(), 
                                                                            lambda x: (x[x == 2] == 2).sum(),
                                                                            lambda x: (x[x == 3] == 3).sum(),
                                                                           ]}),
                                RenameOperator(columns={"value_0": "Asleep",
                                                        "value_1": "Restless",
                                                        "value_2": "Awake"}),
                                CreateFeatureOperator(
                                    feature_name='Bed',
                                    feature_creator=calculate_total_duration 
                                ),                                
                                CreateFeatureOperator(
                                    feature_name='Sleep Efficiency',
                                    feature_creator=calculate_sleep_quality 
                                )
                            ])
# -

aggregate = pipeline.process(df)[0]

aggregate


# +
def categorizeSleepEfficiency(df):
    upper = df[df['Sleep Efficiency'] >= 0.9].index
    intermediate = df[np.logical_and((df['Sleep Efficiency'] >= 0.85), (df['Sleep Efficiency'] < 0.90))].index
    lower = df[df['Sleep Efficiency'] < 0.85].index

    df.loc[upper, "Sleep Efficiency"] = 0
    df.loc[intermediate, "Sleep Efficiency"] = 1
    df.loc[lower, "Sleep Efficiency"] = 2

    return df

    #elif(table['Sleep Efficiency'] >= 0.85 and table['Sleep Efficiency'] <=0.9 )

Y_s = Y_s.groupby('Id').apply(categorizeSleepEfficiency)

# +
# Merge fragmented sleeps
# categorize sleep efficiency
# Plots
# Add sleep codes


# -Generate two timeslots
# --categorize sleep efficiency
# -Add sleep code to two timeslots
# --if end-start > threshold, 1, else 2
# -Add sleep code to main dataframe (single timeslots)
# --merge two timeslots with main dataframe
# -filter to main activity slots activity==1


# -how is x generated: 
# --take main activity from single dataframe, id, time in %H:%M strftime, Activity
# --extract_features per id_logid
# -how is y generated: from two time columns, id, logid, seqid, total minutes in bed, sleep efficinecy
# --yesterday prediction

