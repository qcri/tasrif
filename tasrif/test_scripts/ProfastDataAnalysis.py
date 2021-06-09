# -*- coding: utf-8 -*-
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
import matplotlib.dates as dates
from sklearn import metrics
from datetime import timedelta

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
profast_datapath='/mnt/c/Development/projects/siha/'


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
amount_of_days = 0 # Please Change this parameter if you want to control a quality
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
    df_emr["medlist%d" % idx] = df_emr[medlist].any(axis=1)
    print("Using medlist %d (%s): %d" % (idx, medlist, df_emr["medlist%d" % idx].sum()))
    
medicines = [x for l in list(med_grp.values()) for x in l]
df_emr['medications'] = df_emr[medicines].sum(axis=1)

all_medlists_except_4_and_0 = ['medlist1', 'medlist2', 'medlist3', 'medlist5', 'medlist6', 'medlist7']




# %%
medlist

# %%
medlists = ['medlist0', 'medlist1', 'medlist2', 'medlist3', 'medlist4', 'medlist5', 'medlist6', 'medlist7']
fig, ax = plt.subplots(figsize=(8, 8))
sns.heatmap(df_emr[medlists], ax=ax)

# Compare medlists

# %%
# Is there any patient who's using medlist4 exclusively?
# First condition: using medlist4
# Second condition: every other medlist is false (except medlist0)
df_emr[(df_emr['medlist4'] == True) & (~df_emr[all_medlists_except_4_and_0]).all(axis=1)]

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

# %%
df_emr.loc[32]

# %%
# Sample patient
df_patient = df_cleaned.set_index('patientID')
df_patient = df_patient.loc[32]

# Add day for patient
unique_dates = df_patient['time'].dt.date.unique()
days = list(range(len(unique_dates)))
mapping = dict(zip(unique_dates, days))
df_patient['patient_day'] = df_patient['time'].dt.date.map(mapping)
# df_patient['Day'] = df_patient.index.day
df_patient['Hour'] = df_patient.index.hour

# Sample day
day_zero = df_patient[df_patient['patient_day'] == 0]
day_zero = day_zero.set_index('time')

# Set time as index
df_patient = df_patient.set_index('time')

# %%
df_patient[['HeartRate', 'CGM']].describe()

# %%
df_patient['HeartRate']


# %% [markdown]
# # Does the patient have missing data?

# %%

def plot_patient_days(df, features='CGM'):
    df = df.copy()
    days = df['patient_day'].unique()
    
    # Start plot
    fig, axs = plt.subplots(len(days), 1, figsize=(21, len(days)*2))   
    
    # Normalize features
    if(type(features) == list):
        df[features] = (df[features] - df[features].min()) / (df[features].max() - df[features].min())
        max_y = 1
        min_y = 0
    else:
        describe_y = df[features].describe()
        max_y = describe_y['max']
        min_y = describe_y['min']
    
    for day in days:
        
        df_plot = df[df['patient_day'] == day]
        # Generate index to find missing data
        new_index = pd.date_range(start=df_plot.index.date[0], 
                                  end=df_plot.index.date[0] + timedelta(days=1), 
                                  freq='15T')

        # remove last 15 minutes to avoid moving into a new day
        new_index = new_index[:-1]
        df_plot = df_plot.reindex(new_index)    
        
        # plot multiple lines if list of features
        if(type(features) == list):
            for f in features:
                axs[day].plot(df_plot.index, df_plot[f], linewidth=2)
        else:
            axs[day].plot(df_plot.index, df_plot[features], linewidth=2)

        axs[day].tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=True, rotation=0)
        axs[day].tick_params(axis='x', which='major', labelsize='small')
        axs[day].set_facecolor('snow')

        start_datetime = df_plot.index[0]
        end_datetime = df_plot.index[-1]

        axs[day].set_xlim(start_datetime, end_datetime)
        axs[day].set_ylim(min_y, max_y)

        y_label = df_plot.index[0].strftime('%Y-%m-%d')
        axs[day].set_ylabel("%s" % (y_label,), rotation=0, horizontalalignment="right", verticalalignment="center")

        axs[day].xaxis.set_major_locator(dates.HourLocator(interval=1))
        axs[day].xaxis.set_major_formatter(dates.DateFormatter('%H:%M'))

    # add a legend  
    if(type(features) == list):
        axs[-1].legend(features, loc="lower left")
    else:
        axs[-1].legend([features], loc="lower left")
        
    return fig, axs
        
    
fig, axs = plot_patient_days(df_patient, 'CGM')

# %% [markdown]
# # Visual correlation of features

# %%
fig, axs = plot_patient_days(df_patient, ['mets', 'Steps', 'CGM'])


# %%
# look at peaks, check CGM at t+x
# variability in ts

# %% [markdown]
# # Missing data for patient

# %%
def reindex_df(df):
    new_index = pd.date_range(start=df.index.date[0], 
                              end=df.index.date[0] + timedelta(days=1), 
                              freq='15T')

    # remove last 15 minutes to avoid moving into a new day
    new_index = new_index[:-1]
    df_reindexed = df.reindex(new_index)
    return df_reindexed
        
    
reindexed = df_patient.groupby(df_patient.index.date).apply(reindex_df)
missing_data = reindexed.groupby(level=0)['HeartRate'].apply(lambda x: pd.isnull(x).sum())
print(missing_data)
print('Total missing minutes', missing_data.sum() * 15)

# %%
df_patient.index[0]

# %%
reindexed


# %%
def plot_patient_day(df):
    df = df.copy()
    # Generate index to find missing data
    new_index = pd.date_range(start=df.index.date[0], 
                              end=df.index.date[0] + timedelta(days=1), 
                              freq='15T')
    # remove last 15 minutes of day
    new_index = new_index[:-1]
    
    
    df[['CGM', 'HeartRate']] = (df[['CGM', 'HeartRate']] - df[['CGM', 'HeartRate']].min()) \
        / (df[['CGM', 'HeartRate']].max() - df[['CGM', 'HeartRate']].min())
    
    df_plot = df.reindex(new_index)
    # Normalize features
    
    
    fig, ax = plt.subplots(1, 1, figsize=(21, 8))
    ax.plot(df_plot.index, df_plot['CGM'], linewidth=2)
    ax.plot(df_plot.index, df_plot['mets'], linewidth=2)


    ax.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=True, rotation=0)
    ax.tick_params(axis='x', which='major', labelsize='small')
    ax.set_facecolor('snow')

    start_datetime = df_plot.index[0]
    end_datetime = df_plot.index[-1]

    ax.set_xlim(start_datetime, end_datetime)

    y_label = df_plot.index[0].strftime('%Y-%m-%d')
    ax.set_ylabel("%s CGM" % y_label, rotation=0, horizontalalignment="right", verticalalignment="center")

    ax.xaxis.set_major_locator(dates.HourLocator(interval=1))
    ax.xaxis.set_major_formatter(dates.DateFormatter('%H:%M'))

    # ax.xaxis.set_minor_locator(dates.MinuteLocator(interval=30))
    # ax.xaxis.set_minor_formatter(dates.DateFormatter('%M'))
    # ax.set_yticks([])

    # plt.tick_params(
    #     axis='x',          # changes apply to the x-axis
    #     which='major',      # both major and minor ticks are affected
    #     bottom=False,      # ticks along the bottom edge are off
    #     top=False,         # ticks along the top edge are off
    #     labelbottom=False) # labels along the bottom edge are off
    
    return ax

plot_patient_day(df_patient[df_patient['patient_day'] == 1])




# %% [markdown]
# # Which group have a higher heartrate?

# %%
medlist4_patients = df_emr[df_emr['medlist4']].index.values

# Patient 73 is missing
medlist4_patients
df_cleaned['medlist4_group'] = df_cleaned['patientID'].isin([37, 55, 74, 79])
df_cleaned['Hour'] = df_cleaned['time'].dt.hour
# Plot the results
sns_df = df_cleaned.groupby(['medlist4_group', 'patientID'])['HeartRate'].mean().reset_index()
sns.set_theme(style="ticks")
g = sns.boxplot(y='HeartRate', x='medlist4_group', data=sns_df)

# %% [markdown]
# # During Ramadan?

# %%
sns_df = df_cleaned.groupby(['medlist4_group', 'patientID', 'Ramadan'])['HeartRate'].mean().reset_index()
g = sns.boxplot(y='HeartRate', x='Ramadan', hue='medlist4_group', data=sns_df)

# %% [markdown]
# # Does the patient have more CGM before 6pm or after? (in Ramadan)

# %%
# CGM before VS. after fasting in Ramadan

df_patient_ramadan = df_patient[df_patient['Ramadan'] == 1].copy()
df_patient_ramadan['fasting'] = df_patient_ramadan.index.hour < 18
df_patient_ramadan = df_patient_ramadan.reset_index()
df_patient_ramadan['Day'] = df_patient_ramadan['time'].dt.strftime('%Y-%m-%d')
df_patient_ramadan['Hour'] = df_patient_ramadan['time'].dt.hour


fig, ax = plt.subplots(figsize=(12, 4))
sns.boxplot(y='CGM', x='Day', hue='fasting', data=df_patient_ramadan, ax=ax)
ax.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=True, rotation=0)





# %% [markdown]
# # Does the patient walk more before or after 6pm?

# %%
fig, ax = plt.subplots(figsize=(12, 4))
sns.boxplot(y='Steps', x='Day', hue='fasting', data=df_patient_ramadan, ax=ax)
# ax.set_yscale('log')

# %% [markdown]
# # Which hour does the patient walk more (before VS. during Ramadan)
# The patient seems to walk more afternoon during Ramadan

# %%
fig, ax = plt.subplots(figsize=(12, 4))
sns.boxplot(y='Steps', x='Hour', hue='Ramadan', data=df_patient, ax=ax)

# %%
fig, ax = plt.subplots(figsize=(12, 4))
sns.boxplot(y='CGM', x='Hour', hue='Ramadan', data=df_patient, ax=ax)

# %% [markdown]
# ## For all patients

# %%
fig, ax = plt.subplots(figsize=(12, 4))
sns.boxplot(y='Steps', x='Hour', hue='Ramadan', data=df_cleaned, ax=ax)
ax.set_yscale('log')

# %%
fig, ax = plt.subplots(figsize=(12, 4))
sns.boxplot(y='CGM', x='Hour', hue='Ramadan', data=df_cleaned, ax=ax)
# ax.set_yscale('log')

# %% [markdown]
# # Steps of patients before and after fasting

# %%
df_cleaned_ramadan = df_cleaned[df_cleaned['Ramadan'] == True].copy()
df_cleaned_ramadan['fasting'] = df_cleaned_ramadan['time'].dt.hour < 18
df_cleaned_ramadan = df_cleaned_ramadan.reset_index()
df_cleaned_ramadan['Day'] = df_cleaned_ramadan['time'].dt.strftime('%Y-%m-%d')

# 34 patients
g = sns.catplot(y='Steps', 
                x='Day', 
                hue='fasting', 
                row='patientID', 
                data=df_cleaned_ramadan, 
                kind='box',
                height=3, 
                sharex=False,
                sharey=False,
                aspect=3,
                whis=2.5)

g.set_xticklabels(rotation=35)
g.fig.tight_layout()

for ax in g.axes:
    ax[0].set_yscale('log')

# %%
# group people by medlist
# plot steps

# %%
spacing = np.linspace(-9 * np.pi, 9 * np.pi, num=1000)

s = pd.Series(0.7 * np.random.rand(1000) + 0.3 * np.sin(spacing))

# %% [markdown]
# # DTW - Correlation between Steps and CGM

# %%
from dtw import *


sample_day = df_patient[df_patient['patient_day'] == 1]
alignment = dtw(sample_day['Steps'], sample_day['CGM'], keep_internals=True)



## Display the warping curve, i.e. the alignment curve
alignment.plot(type="threeway")

## Align and plot with the Rabiner-Juang type VI-c unsmoothed recursion
dtw(sample_day['Steps'], sample_day['CGM'], keep_internals=True, 
    step_pattern=rabinerJuangStepPattern(6, "c"))\
    .plot(type="twoway",offset=-2)

## See the recursion relation, as formula and diagram
# print(rabinerJuangStepPattern(6,"c"))
# rabinerJuangStepPattern(6,"c").plot()


# %%
# rolling window -> smoothing steps

# %% [markdown]
# # TLCC - Correlation between Steps and CGM

# %%
def crosscorr(datax, datay, lag=0, wrap=False):
    """ Lag-N cross correlation. 
    Shifted data filled with NaNs 
    
    Parameters
    ----------
    lag : int, default 0
    datax, datay : pandas.Series objects of equal length
    Returns
    ----------
    crosscorr : float
    """
    if wrap:
        shiftedy = datay.shift(lag)
        shiftedy.iloc[:lag] = datay.iloc[-lag:].values
        return datax.corr(shiftedy)
    else: 
        return datax.corr(datay.shift(lag))

d1 = sample_day['Steps']
d2 = sample_day['CGM']
seconds = 5
fps = 30
rs = [crosscorr(d1,d2, lag) for lag in range(-int(seconds*fps),int(seconds*fps+1))]
offset = np.floor(len(rs)/2)-np.argmax(rs)
f,ax=plt.subplots(figsize=(14,3))
ax.plot(rs)
ax.axvline(np.ceil(len(rs)/2),color='k',linestyle='--',label='Center')
ax.axvline(np.argmax(rs),color='r',linestyle='--',label='Peak synchrony')
ax.set(title=f'Offset = {offset} frames\nS1 leads <> S2 leads',ylim=[.1,.31],xlim=[0,301], xlabel='Offset',ylabel='Pearson r')
ax.set_xticks([0, 50, 100, 151, 201, 251, 301])
ax.set_xticklabels([-150, -100, -50, 0, 50, 100, 150]);
plt.legend()

# %%
plt.imshow(acc_cost_matrix.T, origin='lower', cmap='gray', interpolation='nearest')
plt.plot(path[0], path[1], 'w')
plt.xlabel('Subject1')
plt.ylabel('Subject2')
plt.title(f'DTW Minimum Path with minimum distance: {np.round(d,2)}')
plt.show()

# %%
# TODO: medlist 4 VS all in heartrate DONE
# TODO: CGM before iftar VS after DONE
# TODO: for every day, if it has more than 16 hours of data -> consider good
# TODO: Time Lagged Cross Correlation, DTW
# TODO: https://towardsdatascience.com/four-ways-to-quantify-synchrony-between-time-series-data-b99136c4a9c9

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
