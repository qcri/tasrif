# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
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

# # Notebook description
# Goal of this notebook is to compare the data cleaning part done in profast notebook against validators in Tasrif. If Tasrif is able to do the data cleaning part in few lines of intuitive code, then Tasrif can help its users clean the datasets more efficiently
#
# ## Step 1: Create profast functions that cleans the dataset
# modify the variable `profast_datapath` to load profast's `data.json`  

# +
import pandas as pd
from tasrif.processing_pipeline.pandas import DropNAOperator

from tasrif.processing_pipeline.custom import \
    FlagDayIfNotEnoughConsecutiveDaysOperator, \
    FlagDayIfValidEpochsSmallerThanOperator, \
    FlagEpochActivityLessThanOperator, \
    FlagDayIfDaySumSmallerThanOperator, \
    FlagEpochNullColsOperator, \
    ValidationReportOperator, \
    RemoveFlaggedDaysOperator


from tasrif.processing_pipeline import ProcessingPipeline
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

# Modify profast's datapath
profast_datapath='/mnt/c/Development/projects/siha'

# Profast's functions
def read_data(module, class_name_mapping, datapath=profast_datapath):

    ds = module(folder=datapath)

    df_tmp = ds.processed_dataframe()
    df_tmp = df_tmp.reset_index()

    if "time" not in df_tmp:
        df_tmp = df_tmp.rename(columns={"dateTime": "time"})

    df_tmp = df_tmp.rename(columns={"value": class_name_mapping[module]})
    return df_tmp

def drop_days_below_min_steps(df, min_steps=1000, time_col="time", pid_col="patientID"):
    # Get number of steps in a day
    total_steps_day = df.groupby([pid_col, pd.Grouper(key=time_col,freq='D')])["Steps"].sum()

    # Find the <pids, days> to drop
    days_to_drop = total_steps_day[total_steps_day < min_steps].index

    # Temporarily reindex dataframe with <pid, day>
    df_tmp = df.set_index([pid_col, df[time_col].dt.floor("D")])

    # Return only the <pids, days> that are not in the list to drop
    df_tmp = df_tmp.loc[~df_tmp.index.isin(days_to_drop)]

    # The first level of index (pid) needs to come back to the dataframe, but the second one (day) have to be dropped
    return df_tmp.reset_index(level=0).reset_index(drop=True)


# -

# ## Step 2: Load profast's data
# This cell takes a minute or so to finish

# +
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

# The sample interval from the CGM device is very unstable.
df_cgm = read_data(SihaCgmDataset, class_name_mapping)
df_cgm = df_cgm.groupby("patientID")[["CGM", "time"]].resample(rule="15min", offset='00h00min',
                                                               on="time").mean().reset_index()
df_cgm.sort_values(by=["patientID", "time"]).head(20)

df_intra = pd.merge(df_intra, df_cgm)
df_intra.sort_values(by=["patientID", "time"]).head(20)
# -

# ## Step 3: Perform the cleaning using profast's notebook cell

# +
df_cleaned = df_intra.copy()
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
df_cleaned
# -

# ## Step 4: Perform the same data cleaning methods by using Tasrif's validators
# Compare the results with the above cell

# +
df_cleaned = df_intra.copy()

print("\t* Total number of epochs: %d" % df_cleaned.shape[0])

print("--- Removing days in which number of steps is smaller than 1000 (for the whole day)")

validator_pipeline = ProcessingPipeline([
                        DropNAOperator(subset=['CGM']),
                        FlagDayIfDaySumSmallerThanOperator(col='Steps', 
                                                           sum_threshold=1000,
                                                           time_col='time', 
                                                           pid_col='patientID',),
                        RemoveFlaggedDaysOperator(),
                     ])


df_cleaned = validator_pipeline.process(df_cleaned)[0]
print("\t* Remaining number of epochs: %d" % df_cleaned.shape[0])
df_cleaned
