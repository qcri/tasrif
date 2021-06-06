# -*- coding: utf-8 -*-
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
#     display_name: PyCharm (tasrif)
#     language: python
#     name: pycharm-5bd30262
# ---

# %% [markdown]
# # Profast Data Analysis
#
# This notebook is divided into two main sections:
# 1. [Data and Correlation Analysis and Preprocessing](#data_analysis)
# 2. [Machine Learning for CGM Prediction](#machine_learning)

# %% [markdown]
# <a id='data_analysis'></a>
# ## 1. Data and Correlation Analysis and Preprocessing

# %%
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from dataprep.eda import create_report
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn import metrics

from tasrif.data_readers.siha_dataset import \
    SihaSleepDataset, \
    SihaStepsIntradayDataset, \
    SihaCaloriesIntradayDataset, \
    SihaDistanceIntradayDataset, \
    SihaHeartRateIntradayDataset, \
    SihaVeryActiveMinutesDataset, \
    SihaLightlyActiveMinutesDataset, \
    SihaSedentaryMinutesDataset, \
    SihaModeratelyActiveMinutesDataset, \
    SihaTimeInHeartRateZonesDataset, \
    SihaStepsDataset, \
    SihaCaloriesDataset, \
    SihaDistanceDataset, \
    SihaCgmDataset, \
    SihaEmrDataset, \
    SihaSleepIntradayDataset

# %% [markdown]
# The following location is where I mounted the driver in my computer.
# Change it accordingly.

# %% pycharm={"name": "#%%\n"}
profast_datapath='../../data/profast2020/'


# %% [markdown]
# Below, we defined a set of helper functions for the analysis.
# Some of them could be eventually integrated back to Tasrif

# %%
# Helper functions
def read_data(module, class_name_mapping, datapath=profast_datapath):

    ds = module(folder=datapath)

    df_tmp = ds.processed_dataframe()
    df_tmp = df_tmp.reset_index()

    if "time" not in df_tmp:
        df_tmp = df_tmp.rename(columns={"dateTime": "time"})

    df_tmp = df_tmp.rename(columns={"value": class_name_mapping[module]})
    return df_tmp


def ramadan_flag(t, start_ramadan=pd.Timestamp(2020, 4, 23, 0, 0, 0),
                 end_ramadan=pd.Timestamp(2020, 5, 23, 23, 59, 59)):

    if (t >= start_ramadan) & (t <= end_ramadan):
        return 1
    elif t < start_ramadan:
        return 0
    else:
        return 2

def get_participant(df, pid):
    return df[df["patientID"] == pid]

def get_before_ramadan(df):
    return df[df["Ramadan"] == 0]

def get_ramadan(df):
    return df[df["Ramadan"] == 1]

def corr_per_day(df):
    return df.groupby(pd.Grouper(key='time',freq='D'))[["Calories", "mets", "Steps", "Distance", "CGM"]].corr()

def get_data_day(df, month, day, time_col="time"):
    return df[(df[time_col].dt.month == month) & (df[time_col].dt.day == day)]

def drop_days_below_min_steps(df, min_steps=1000, time_col="time", pid_col="patientID"):
    # Get number of steps in a day
    total_steps_day = df.groupby([pid_col, pd.Grouper(key=time_col,freq='D')])["Steps"].sum()

    # Find the <pids, days> to drop
    days_to_drop = total_steps_day[total_steps_day <= min_steps].index

    # Temporarily reindex dataframe with <pid, day>
    df_tmp = df.set_index([pid_col, df[time_col].dt.floor("D")])

    # Return only the <pids, days> that are not in the list to drop
    df_tmp = df_tmp.loc[~df_tmp.index.isin(days_to_drop)]

    # The first level of index (pid) needs to come back to the dataframe, but the second one (day) have to be dropped
    return df_tmp.reset_index(level=0).reset_index(drop=True)


def agg_per_day(df, metric, operation, outputcol_name=None, pid_col="patientID", time_col="time", 
                remove_zero_steps=False):

    if remove_zero_steps:
        df_tmp = df[df["Steps"] > 0]
    else:
        df_tmp = df.copy()

    # Get aggregated number of [steps, calories, ...] in a day 
    metric_per_day = df_tmp.groupby([pid_col, pd.Grouper(key=time_col,freq='D')])[metric].agg(operation)
    if outputcol_name is not None:
        metric_per_day.name = outputcol_name
    return metric_per_day

def sum_per_day(df, metric, outputcol_name=None, pid_col="patientID", time_col="time", remove_zero_steps=False):
    return agg_per_day(df, metric, "sum", outputcol_name, pid_col, time_col, remove_zero_steps)

def mean_per_day(df, metric, outputcol_name=None, pid_col="patientID", time_col="time", remove_zero_steps=False):
    return agg_per_day(df, metric, "mean", outputcol_name, pid_col, time_col, remove_zero_steps)

def is_mild_hypo(cgm_value):
    if cgm_value <= 70: 
        return 1
    return 0

def is_severe_hypo(cgm_value):
    if cgm_value <= 54: 
        return 1
    return 0

def is_hyper(cgm_value):
    if cgm_value >= 180: 
        return 1
    return 0

def hyper_hypo_label(cgm_value):
    if is_hyper(cgm_value):
        return 1
    elif is_hypo(cgm_value):
        return 2
    return 0


# %% [markdown]
# ### First Goal: Combining intra day data
#
# The cells below aim to combine the intra day FitBit and CGM data

# %% pycharm={"name": "#%%\n"}
class_name_mapping = {SihaCaloriesIntradayDataset: "Calories",
                      SihaStepsIntradayDataset: "Steps",
                      SihaDistanceIntradayDataset: "Distance",
                      SihaHeartRateIntradayDataset: "HeartRate",
                      SihaCgmDataset: "CGM"
}

# Read HR data:
df_intra = read_data(SihaHeartRateIntradayDataset, class_name_mapping)
# Resample to 15 min intervals
df_intra = df_intra.groupby("patientID")[["HeartRate", "time"]].resample(rule="15min",
                                                                         offset='00h00min',
                                                                         on="time").mean().reset_index()

for intraday_module in [SihaCaloriesIntradayDataset, SihaStepsIntradayDataset,
                         SihaDistanceIntradayDataset]:

    df_tmp = read_data(intraday_module, class_name_mapping)
    df_intra = pd.merge(df_intra, df_tmp, on=['patientID', 'time'], how='outer')

df_intra = df_intra.dropna()
df_intra.head(10)

# %% [markdown]
# Following, we sync'ed the CGM data on a 15min interval round on the hour (i.e., HH:00, HH:15, HH:30, HH:45)

# %% pycharm={"name": "#%%\n"}
# The sample interval from the CGM device is very unstable.
df_cgm = read_data(SihaCgmDataset, class_name_mapping)

# %%
# Let's have a look at the data for a given patient 
df_sample = df_cgm[df_cgm["patientID"] == 27]
df_sample.sort_values(by=["time"]).head(20)
# note how there is a big jump from 12:17 to 15:19 for pid 27

# %% [markdown]
# We force the data to have a 15 min pattern using the mean of all values inside the 15 win.
# That is important for the data alignment for the rest of this work

# %%
df_cgm = df_cgm.groupby("patientID")[["CGM", "time"]].resample(rule="15min", offset='00h00min',
                                                               on="time").mean().reset_index()

df_cgm.sort_values(by=["patientID", "time"]).head(20)

# %% [markdown]
# Merge the whole intraday data

# %% pycharm={"name": "#%%\n"}
df_intra = pd.merge(df_intra, df_cgm)
df_intra.sort_values(by=["patientID", "time"]).head(20)

# %% [markdown]
# Next, we add a Ramadan annotation. The ramadan date can be adjusted if wrongly set.

# %%
df_intra["Ramadan"] = df_intra["time"].apply(ramadan_flag)

# %% [markdown]
# Augment dataset with Hypo/Hyper annotations

# %%

df_intra["hypo"] = df_intra["CGM"].apply(lambda x: is_mild_hypo(x))
df_intra["shypo"] = df_intra["CGM"].apply(lambda x: is_severe_hypo(x))
df_intra["hyper"] = df_intra["CGM"].apply(lambda x: is_hyper(x))

# %% [markdown]
# ### Data cleaning

# %% [markdown]
# Next, we are going to perform a few data cleaning procedures.
# Note that a large number of epochs (almost 43% of all FitBit datapoints) miss the corresponding CGM data.
# As we are going to later predict CGM, we will now drop invalid CGM data instead of imputing it.

# %%
df_cleaned = df_intra.copy()

# %%
print("Initial number of participants: %d" % df_cleaned["patientID"].unique().shape[0])
print("Total number of epochs: %d" % df_cleaned.shape[0])
print("Total number of epochs without CGM: %d (%.2f%% of total)" % (df_cleaned["CGM"].isnull().sum(),
                                                                    100.* df_cleaned["CGM"].isnull().sum()/df_intra.shape[0]))

print("--- Removing invalid CGM epochs....")
df_cleaned = df_cleaned.dropna()
print("\t* Remaining number of epochs: %d" % df_cleaned.shape[0])

print("--- Removing days in which number of steps is smaller than 1000 (for the whole day)")
df_cleaned = drop_days_below_min_steps(df_cleaned)

print("\t* Remaining number of epochs: %d" % df_cleaned.shape[0])

# %% [markdown]
# To be able to better understand the data that we have, we verify how many valid days we have previously, during and after ramadan.

# %% pycharm={"name": "#%%\n"}
# Table: how many pre-ramadan, during ramadan, post-ramadan per user we have
rows = []
for pid in df_cleaned["patientID"].unique():
    df_tmp = df_cleaned[df_cleaned["patientID"] == pid]
    row = {}
    row["pid"] = pid
    row["before"] = df_tmp[(df_tmp["Ramadan"] == 0)]["time"].dt.floor("d").unique().shape[0]
    row["during"] = df_tmp[(df_tmp["Ramadan"] == 1)]["time"].dt.floor("d").unique().shape[0]
    row["after"] = df_tmp[(df_tmp["Ramadan"] == 2)]["time"].dt.floor("d").unique().shape[0]
    row["total"] = row["before"] + row["during"] + row["after"]
    rows.append(row)

data_availability = pd.DataFrame(rows)
data_availability


# %%
amount_of_days = 5

rows = []
for amount_of_days in range(0, 11):
        remaining = data_availability[(data_availability["before"] >= amount_of_days) & (data_availability["during"] >= amount_of_days)]
        row = {}
        row["#Days"] = amount_of_days
        row["#Users"] = remaining.shape[0]
        rows.append(row)
r = pd.DataFrame(rows)
print(r)
r.plot(x="#Days")

# %%
amount_of_days = 0
good_avail = data_availability[(data_availability["before"] >= amount_of_days) & (data_availability["during"] >= amount_of_days)]
good_avail

# %% [markdown]
# Keep only participants with good data avilability

# %%
print("Total number of epochs so far: %d" % df_cleaned.shape[0])
df_cleaned = df_cleaned[df_cleaned["patientID"].isin(good_avail["pid"].to_list())]
print("\t* Remaining number of epochs: %d" % df_cleaned.shape[0])
print("\t* Remaining number of participants: %d" % df_cleaned["patientID"].unique().shape[0])

print("More than 5 days both before and during ramadan for %d participants." % (good_avail.shape[0]))
print("Ids: %s" % good_avail["pid"].to_list())


# %% [markdown]
# ### Data Viz
#
# Unfortunately, simple matplotlib/pandas viz do not work properly, as we have two separated data periods, as we can see by the example below:

# %%
d = get_participant(df_cleaned, 39)
d.plot(x="time", y=["Steps", "Calories", "CGM"])

# %% [markdown]
# Alternatively, we can split the data into two different dataframes (df_before_ramanda and df_ramadan) and analyse them separately with dataprep, for example:

# %%
df_before_ramadan = corr_per_day(get_before_ramadan(d))
df_ramadan = corr_per_day(get_ramadan(d))

# %% [markdown]
# I could not spot any clear difference between the datasets:

# %%
df_ramadan = get_before_ramadan(df_cleaned).reset_index(drop=True)
if "Ramadan" in df_ramadan:
    del df_ramadan["Ramadan"]

create_report(df_ramadan)

# %%
df_ramadan = get_ramadan(df_cleaned).reset_index(drop=True)
df_ramadan["hypo"] = df_ramadan["hypo"].astype(int)
if "Ramadan" in df_ramadan:
    del df_ramadan["Ramadan"]

create_report(df_ramadan)


# %% [markdown]
# # Before Vs During Ramadan

# %%
def boxplot_distribution(df, main_col="CGM", operator="mean", pid_col="patientID",
                         time_col="time", remove_zero_steps=True):

    if remove_zero_steps:
        df_tmp = df[df["Steps"] > 0].copy()
    else:
        df_tmp = df.copy()

    # Get day correlation
    df_tmp["Date"] = df_tmp[time_col].dt.date
    df_tmp = df_tmp.groupby([pid_col, "Date"])[main_col].agg(operator).reset_index()
    
    sns.set_theme(style="ticks")
    
    # Plot the orbital period with horizontal boxes
    df_tmp["Ramadan"] = df_tmp["Date"].apply(lambda x: ramadan_flag(x))

    g = sns.boxplot(x=pid_col, y=main_col, hue="Ramadan", data=df_tmp)
    sns.stripplot(x=pid_col, y=main_col, data=df_tmp,
                  size=4, color=".3", linewidth=0)
    
    #g.set(ylabel='Pearson Correlation\n%s-%s' % (main_col, secondary_col))
    
    sns.despine(offset=10, trim=True)
    
    # Move the legend to the right side
    g.legend(title="Ramadan?", bbox_to_anchor=(1.3, 0.5), ncol=1)
    
    return g



# %%
# TODO:
# Percentage difference before vs during ramadan
# Check it out if periods of day have differences ()
# Pulse rate is lower if they are fasting or just doing less PA
# Delayed correlation rate (PA correated with delayed CGM X hours ahead)
# Missing PIDs

# %%
boxplot_distribution(df_cleaned, "HeartRate", operator="mean", remove_zero_steps=False)

# %%
boxplot_distribution(df_cleaned, "Calories", operator="mean", remove_zero_steps=False)

# %%
boxplot_distribution(df_cleaned, "mets", operator="mean", remove_zero_steps=False)

# %%
boxplot_distribution(df_cleaned, "Steps", operator="mean", remove_zero_steps=False)

# %%
boxplot_distribution(df_cleaned, "CGM", operator="mean", remove_zero_steps=False)


# %% [markdown]
# ## Correlation Analysis

# %% [markdown]
# ### Correlations at day-level for each user
#
# Answering questions like: In a given day, how do the CGM and Number of Steps correlate for a given user?

# %%
def boxplot_delayed_correlation_with_filter(df, secondary_col, main_col="CGM",
                                            pid_col="patientID", time_col="time", time_delay="0h15t",
                                            remove_zero_steps=True, 
                                            # Optional data
                                            agg_operator="mean", main_min_filter=0, secondary_min_filter=0):

    if remove_zero_steps:
        df_tmp = df[df["Steps"] > 0].copy()
    else:
        df_tmp = df.copy()

    shifted_col_name = "Shifted_" + time_delay + "_" + secondary_col
        
    # Group data by patient, select a column and shift dataset 
    df_shifted = df_tmp.set_index(time_col).groupby(pid_col)[[secondary_col]].shift(freq=time_delay).reset_index()
    df_shifted = df_shifted.rename(columns={secondary_col: shifted_col_name})
    # Merge back the shifted dataset
    df_tmp = pd.merge(df_tmp, df_shifted, on=["patientID", "time"])
    
    # Get day correlation and apply operator
    df_tmp["Date"] = df_tmp[time_col].dt.date
    
    df_corr = df_tmp.groupby([pid_col, "Date"])[[main_col, shifted_col_name]].corr().reset_index()
    df_corr = df_corr[df_corr["level_2"] == main_col]
    
    df_mean = df_tmp.groupby([pid_col, "Date"])[[main_col, shifted_col_name]].agg(agg_operator).reset_index()
    
    # merge temporary dataframes
    df_tmp = pd.merge(df_corr, df_mean, on=[pid_col, "Date"], suffixes=("", "_" + agg_operator))
    
    # optionally filters rows that are below the minimal defined by `main_min_filter` and `secondary_min_filter`
    df_tmp = df_tmp[df_tmp[main_col + "_" + agg_operator] >= main_min_filter]
    df_tmp = df_tmp[df_tmp[shifted_col_name + "_" + agg_operator] >= secondary_min_filter]
    
    # After all data modifications, we add the Ramadan flag
    df_tmp["Ramadan"] = df_tmp["Date"].apply(lambda x: ramadan_flag(x))
    
    # Plot the results    
    sns.set_theme(style="ticks")
    
    # Plot the orbital period with horizontal boxes
    g = sns.boxplot(x=pid_col, y=shifted_col_name, hue="Ramadan", data=df_tmp)
    sns.stripplot(x=pid_col, y=shifted_col_name, data=df_tmp,
                  size=4, color=".3", linewidth=0)
    
    g.set(ylabel='Pearson Correlation\n%s-%s' % (main_col, shifted_col_name))
    
    sns.despine(offset=10, trim=True)
    
    # Move the legend to the right side
    g.legend(title="Ramadan?", bbox_to_anchor=(1.3, 0.5), ncol=1)
    
    # Remove useless cols that were added by the correlation operator
    del df_tmp["level_2"]
    del df_tmp[main_col]
    
    return df_tmp, g
    


# %%
# correlations = {}

# for time_delay in ["0h00t", "1h00t", "2h00t", "3h00t", "4h00t", "5h00t", "6h00t", "7h00t", "8h00t"]:
#     c, g = boxplot_delayed_correlation(df_cleaned, "mets", remove_zero_steps=False, time_delay=time_delay)
#     correlations[time_delay] = c


# %%
boxplot_delayed_correlation_with_filter(df_cleaned, main_col="CGM", secondary_col="CGM",
                                        remove_zero_steps=False, time_delay="4h00t", 
                                        agg_operator="mean", main_min_filter=0, secondary_min_filter=0)

# %%
df_cleaned["mets"].describe()

# %%
boxplot_delayed_correlation_with_filter(df_cleaned, "mets", remove_zero_steps=False, time_delay="5h00t")

# %%
boxplot_delayed_correlation_with_filter(df_cleaned, "Steps")

# %%
boxplot_delayed_correlation_with_filter(df_cleaned, "Calories")

# %%
boxplot_delayed_correlation_with_filter(df_cleaned, "HeartRate")

# %% [markdown]
# ### Correlations at user level
#
# Next, our aim is to create correlation at user level. For that, we first aggregate the data at day level (e.g., average number of steps in a day) and then average the data at user level (i.e., average number of steps per day for participant X).
#
# For that, we will first load the EMR data and do a few data cleaning steps.

# %% [markdown]
# #### Load EMR data, clean and pivot it

# %%
# df_emr = read_data(SihaEmrDataset, {SihaEmrDataset: "value"}).drop_duplicates(["patientID", "variable", "value"])
# df_emr = df_emr.pivot(index="patientID", columns="variable", values="value")

# emr_cols = ['BMI', 'Cholesterol', 'Creatinine', 'Diabetes Duration',
#             'Diastolic Blood Pressure', 'HDL', 'HbA1c', 'LDL', 'Systolic Blood Pressure']

# # For some reason this was understood as a string col and we need to replace empty string to NAN manually
# df_emr["Diabetes Duration"] = df_emr["Diabetes Duration"].replace('', np.nan)

# for emr_col in emr_cols:
#     print(emr_col)
#     df_emr[emr_col] = df_emr[emr_col].astype(np.float)

# df_emr.head()
# # TODO: get pids of missing EMR data

# %%
# HACK
df_emr = read_data(SihaEmrDataset, {SihaEmrDataset: "value"}).drop_duplicates(["patientID", "variable", "value"])
df_emr = df_emr.drop(index=[28,29])

df_emr = df_emr.pivot(index="patientID", columns="variable", values="value")

emr_cols = ['BMI', 'Cholesterol', 'Creatinine', 'Diabetes Duration',
            'Diastolic Blood Pressure', 'HDL', 'HbA1c', 'LDL', 'Systolic Blood Pressure']

# For some reason this was understood as a string col and we need to replace empty string to NAN manually
df_emr["Diabetes Duration"] = df_emr["Diabetes Duration"].replace('', np.nan)

for emr_col in emr_cols:
    print(emr_col)
    df_emr[emr_col] = df_emr[emr_col].astype(np.float)

# Mistakes that I did and tried to fix
df_emr.loc[71, "Systolic Blood Pressure"] = 120
df_emr.loc[71, "Diastolic Blood Pressure"] = 70

# Still missing some data
df_emr["Diabetes Duration"].fillna(df_emr["Diabetes Duration"].mean(), inplace=True)
df_emr["Triglyceride"] = df_emr["Triglyceride"].astype(float)
df_emr["Triglyceride"].fillna(df_emr["Triglyceride"].mean(), inplace=True)

df_emr.head()

# %%
# Missing data listed below:
df_emr[df_emr.isnull().any(axis=1)]

# %%
has_any_missing_emr = df_emr[df_emr.isnull().any(axis=1)].index
set(df_cleaned["patientID"].unique()) & set(has_any_missing_emr) # fillna for this guy

# %%
# Tried to normalize the drug names
drugs = set([])
for tmp in df_emr["Diabetes Medication"]:
    for d in tmp.replace("+", ", ").split(","):
        if d.strip():
            # This should get rid of the dosage (Glargine 300 or Glargine 100 -> Glargine)
            d = d.split()[0].strip()
            drugs.add(d)


print("%d different drugs: %s" % (len(drugs), drugs))

for d in drugs:
    df_emr[d] = df_emr["Diabetes Medication"].apply(lambda x: d in x)

df_emr.head()

# %%
med_grp = {}
med_grp[0] = ['Metformin']
med_grp[1] = ['Glicalzide', 'Glimepride']
med_grp[2] = ['Vildagliptin', 'Sitagliptin']
med_grp[3] = ['Dapagliflozin', 'Empagliflozin']
med_grp[4] = ['Liraglutide', 'Exenatide']
med_grp[5] = ['Glargine', 'Degludec']
med_grp[6] = ['Aspart']
med_grp[7] = ['Pioglitazone']

# %%
for idx, medlist in med_grp.items():
    df_emr["medlist%d" % idx] = df_emr[medlist].all(axis=1)
    print("Using medlist %d (%s): %d" % (idx, medlist, df_emr["medlist%d" % idx].sum()))


# %%
df_emr.head()

# %%
df_agg = pd.concat([mean_per_day(df_cleaned, "Steps", "MeanSteps", remove_zero_steps=True),
                      sum_per_day(df_cleaned, "Steps", "SumSteps", remove_zero_steps=True),
                      mean_per_day(df_cleaned, "Calories", "MeanCalories", remove_zero_steps=True),
                      sum_per_day(df_cleaned, "Calories", "SumCalories", remove_zero_steps=True),
                      mean_per_day(df_cleaned, "Distance", "MeanDistance", remove_zero_steps=True),
                      sum_per_day(df_cleaned, "Distance", "SumDistance", remove_zero_steps=True),
                      mean_per_day(df_cleaned, "mets", "MeanMETS", remove_zero_steps=True),
                      mean_per_day(df_cleaned, "HeartRate", "MeanHeartRate", remove_zero_steps=True),
                      mean_per_day(df_cleaned, "CGM", "MeanCGM", remove_zero_steps=True),
                      mean_per_day(df_cleaned, "hypo", "Mean#Hypo", remove_zero_steps=True),
                      sum_per_day(df_cleaned, "hypo", "TotalHypo", remove_zero_steps=True),
                      mean_per_day(df_cleaned, "hyper", "Mean#Hyper", remove_zero_steps=True),
                      sum_per_day(df_cleaned, "hyper", "TotalHyper", remove_zero_steps=True),
                      mean_per_day(df_cleaned, "Ramadan", "IsRamadan", remove_zero_steps=True),
                      ], axis=1).reset_index()
df_means = df_agg.groupby("patientID").mean()
df_means.head()

# %%
df_merged = pd.merge(df_means, df_emr, left_index=True, right_index=True)
df_merged.head()

# %%
print("Missing participants: ", set(df_means.index) - set(df_emr.index))

# %%
total_participants = df_merged.shape[0]
print("Total number of participants at this analysis: %d" % (total_participants))
for d in drugs:
    df_drug = df_merged[df_merged[d] == True]
    print("Participants in the final dataset that took drug %s: %d (%.2f%%)" % (d, df_drug.shape[0], 100.*df_drug.shape[0]/total_participants))

# %% [markdown]
# We are now able to compare the correlation between any number of groups.
# For example, below we verify that the correlation between average CGM and HeartRate was strong for participants taking the combo of Metformin and Sitagliptin, while it was the opposite for participants not taking these drugs. 
#
# ⭐️⭐️⭐️ <b> TODO: we need to know what kind of medicament combination is important! </b> ⭐️⭐️⭐️

# %%
# Compared the data for the 8 participants that took Sitagliptin against the 4 ones that did not take it
df_grp1 = df_merged[(df_merged['Liraglutide'] == True)]
df_grp2 = df_merged[~((df_merged['Liraglutide'] == True))]

print("Corr CGM-HeartRate Grp1: %.3f, Grp2: %.3f" % (df_grp1.corr()["MeanCGM"]["MaxHeartRate"],
                                                     df_grp2.corr()["MeanCGM"]["MaxHeartRate"]))

# %% [markdown]
# Before moving to the next section, we save a postprocessed dataframe to disk in order to speed up futher ML pipelines.

# %%
profast_ml = os.path.join(profast_datapath, "preprocessed")
if not os.path.exists(profast_ml):
    os.mkdir(profast_ml)

df_cleaned.to_csv(os.path.join(profast_ml, "df_cleaned.csv.gz"), index=False)
df_emr.to_csv(os.path.join(profast_ml, "df_cleaned_emr.csv.gz"))
