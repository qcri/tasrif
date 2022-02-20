# ---
# jupyter:
#   jupytext:
#     formats: py:percent,ipynb
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

# %% [markdown] slideshow={"slide_type": "slide"}
# # Goal of the notebook
# - To start from a raw file, and end on producing X, y datasets for a machine learning model
#

# %% slideshow={"slide_type": "skip"}
"""Example on how to read all data from SIHA
"""
import os

import numpy as np
import pandas as pd
import seaborn as sns
from dataprep.eda import create_report

from tasrif.data_readers.siha_dataset import SihaDataset
from tasrif.processing_pipeline import (
    ComposeOperator,
    NoopOperator,
    ProcessingOperator,
    SequenceOperator,
    SplitOperator,
)
from tasrif.processing_pipeline.custom import (
    AggregateOperator,
    CreateFeatureOperator,
    EncodeCyclicalFeaturesOperator,
    FilterOperator,
    FlattenOperator,
    JqOperator,
    ResampleOperator,
    SetFeaturesValueOperator,
    SlidingWindowOperator,
)
from tasrif.processing_pipeline.kats import CalculateTimeseriesPropertiesOperator
from tasrif.processing_pipeline.map_processing_operator import MapProcessingOperator
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConcatOperator,
    ConvertToDatetimeOperator,
    CorrOperator,
    DropDuplicatesOperator,
    DropFeaturesOperator,
    DropNAOperator,
    FillNAOperator,
    GroupbyOperator,
    JsonNormalizeOperator,
    MeanOperator,
    MergeOperator,
    PivotOperator,
    RenameOperator,
    ResetIndexOperator,
    SetIndexOperator,
    SortOperator,
)
from tasrif.processing_pipeline.tsfresh import TSFreshFeatureExtractorOperator

siha_folder_path = os.environ["SIHA_PATH"]


class UnstackOperator(MapProcessingOperator):
    def _processing_function(self, series):
        return series.unstack()


class SizeOperator(MapProcessingOperator):
    def _processing_function(self, df):
        return df.size()


class ApplyOperator(ProcessingOperator):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def process(self, *data_frames):
        processed = []
        for data_frame in data_frames:
            data_frame = data_frame.apply(**self.kwargs)
            processed.append(data_frame)

        return processed


class TransformOperator(ProcessingOperator):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def process(self, *data_frames):
        processed = []
        for data_frame in data_frames:
            data_frame = data_frame.transform(**self.kwargs)
            processed.append(data_frame)

        return processed


def ramadan_flag(
    t,
    start_ramadan=pd.Timestamp(2020, 4, 23, 0, 0, 0),
    end_ramadan=pd.Timestamp(2020, 5, 23, 23, 59, 59),
):

    if (t >= start_ramadan) & (t <= end_ramadan):
        return 1
    elif t < start_ramadan:
        return 0
    else:
        return 2


import pandas as pd
from tqdm import tqdm

from tasrif.processing_pipeline import ProcessingOperator

# %% [markdown] slideshow={"slide_type": "subslide"}
# # Extract EMR, CGM, steps, distance, and calories
#
# ![Capture4-2.PNG](attachment:Capture4-2.PNG)

# %% slideshow={"slide_type": "subslide"}
base_datasets = SequenceOperator(
    [
        SihaDataset(folder_path=siha_folder_path, table_name="EMR"),
        ComposeOperator(
            [
                JqOperator("map({patientID} + .data.emr[])"),  # EMR
                JqOperator("map({patientID} + .data.cgm[])"),  # CGM
                JqOperator(
                    'map({patientID} + .data.activities_tracker_steps[].data."activities-tracker-steps"[0])'
                ),  # Steps
                JqOperator(
                    'map({patientID} + .data.activities_tracker_distance[].data."activities-tracker-distance"[0])'
                ),  # Distance
                JqOperator(
                    'map({patientID} + .data.activities_tracker_calories[].data."activities-tracker-calories"[0])'
                ),  # Calories
            ]
        ),
        FlattenOperator(),
        JsonNormalizeOperator(),  # Converts Json to a dataframe
        RenameOperator(columns={"time": "dateTime"}, errors="ignore"),
        ConvertToDatetimeOperator(
            feature_names=["dateTime"], infer_datetime_format=True
        ),
        SetIndexOperator("dateTime"),
        AsTypeOperator({"value": "float32"}, errors="ignore"),
    ]
)

df = base_datasets.process()

# %% slideshow={"slide_type": "skip"}
# intraday_datasets = ["HeartRateIntraday", "CaloriesIntraday", "StepsIntraday", "DistanceIntraday"]
# does the user have to know JqOperator syntax? Is there a way to make things easier?
intraday_datasets = SequenceOperator(
    [
        SihaDataset(folder_path=siha_folder_path, table_name="HeartRateIntraday"),
        ComposeOperator(
            [
                JqOperator(
                    "map({patientID} + .data.activities_heart_intraday[].data as $item  | "
                    + '$item."activities-heart-intraday".dataset | '
                    + 'map({date: $item."activities-heart"[0].dateTime} + .) | .[])'
                ),  # HeartRateIntraday
                JqOperator(
                    "map({patientID} + .data.activities_calories_intraday[].data as $item  |"
                    + ' $item."activities-calories-intraday".dataset | '
                    + 'map({date: $item."activities-calories"[0].dateTime} + .) | .[])'
                ),  # CaloriesIntraday
                JqOperator(
                    "map({patientID} + .data.activities_steps_intraday[].data as $item  | "
                    + '$item."activities-steps-intraday".dataset | '
                    + 'map({date: $item."activities-steps"[0].dateTime} + .) | .[])'
                ),  # StepsIntraday
                JqOperator(
                    "map({patientID} + .data.activities_distance_intraday[].data as $item  |"
                    + ' $item."activities-distance-intraday".dataset | '
                    + 'map({date: $item."activities-distance"[0].dateTime} + .) | .[])'
                ),  # DistanceIntraday
            ]
        ),
        FlattenOperator(),
        JsonNormalizeOperator(),
        CreateFeatureOperator(
            feature_name="dateTime",
            feature_creator=lambda df: df["date"] + "T" + df["time"],
        ),
        DropFeaturesOperator(["date", "time"]),
        ConvertToDatetimeOperator(
            feature_names=["dateTime"], infer_datetime_format=True
        ),
        SetIndexOperator("dateTime"),
        AsTypeOperator({"value": "float32"}, errors="ignore"),
        SplitOperator(
            [
                RenameOperator(columns={"value": "HeartRate"}),
                RenameOperator(columns={"value": "Calories"}),
                RenameOperator(columns={"value": "Steps"}),
                RenameOperator(columns={"value": "Distance"}),
            ]
        ),
        FlattenOperator(),
    ]
)

df_intra = intraday_datasets.process()

# %% slideshow={"slide_type": "slide"}
# Extracted data with periodicity of 1-minute
df_intra[0]

# %% slideshow={"slide_type": "slide"}
# While other datasets may have different periodicity
df_intra[1]

# %% [markdown] slideshow={"slide_type": "subslide"}
# # How do we resample the time column to 15 minutes per patient?
# - Group by patient
# - Resample the time to 15 minute
# - take the mean of the heartrate

# %% slideshow={"slide_type": "skip"}
df_hr = df_intra[0]
df_hr

# %% slideshow={"slide_type": "subslide"}
# Resample Heartrate time column to 15min per patient
hr_pipeline = SequenceOperator(
    [
        GroupbyOperator(by="patientID"),
        ResampleOperator(rule="15min", aggregation_definition={"HeartRate": "mean"}),
        ResetIndexOperator(level=0),
    ]
)

# %% slideshow={"slide_type": "subslide"}
# Resampled to 15 minutes
df_hr = hr_pipeline.process(df_hr)[0]
df_intra[0] = df_hr
df_hr

# %% slideshow={"slide_type": "skip"}
df_cgm = df[1]

# CGM resample pipeline
CGM_pipeline = SequenceOperator(
    [
        RenameOperator(columns={"value": "CGM"}),
        GroupbyOperator(by="patientID"),
        ResampleOperator(rule="15min", aggregation_definition={"CGM": "mean"}),
        ResetIndexOperator(level=0),
    ]
)

df_cgm = CGM_pipeline.process(df_cgm)[0]

# %% slideshow={"slide_type": "subslide"}
# Merge Intraday datasets
# Operators can be called outside of Pipeline class
df_intra = (
    MergeOperator(on=["patientID", "dateTime"], how="outer")
    .process(*df_intra, df_cgm)
    .dropna()
)

df_intra


# %% slideshow={"slide_type": "skip"}
feature_creator_pipeline = SequenceOperator(
    [
        ResetIndexOperator(),
        CreateFeatureOperator(
            feature_name="Ramadan",
            feature_creator=lambda df: ramadan_flag(df["dateTime"]),
        ),
        CreateFeatureOperator(
            feature_name="hypo", feature_creator=lambda df: df["CGM"] <= 70
        ),
        CreateFeatureOperator(
            feature_name="hyper", feature_creator=lambda df: df["CGM"] >= 180
        ),
    ]
)

df_intra = feature_creator_pipeline.process(df_intra)[0]


# %% [markdown] slideshow={"slide_type": "subslide"}
# # We'd like the dataframe to have patients with consecutive data and variable number of steps
# - Consecutive days >= 3
# - min steps > 2000
# - Our data has a periodicity of 15 minutes

# %% slideshow={"slide_type": "subslide"}
filter_pipeline = FilterOperator(
    participant_identifier="patientID",
    date_feature_name="dateTime",
    day_filter={
        "column": "Steps",
        "filter": lambda day_steps: day_steps.sum() >= 2000,
        "consecutive_days": 3,
    },
    filter_type="include",
)


df_intra = filter_pipeline.process(df_intra)[0]
df_intra


# %% slideshow={"slide_type": "subslide"}
# Original code


def drop_days_below_min_steps(df, min_steps=1000, time_col="time", pid_col="patientID"):
    # Get number of steps in a day
    total_steps_day = df.groupby([pid_col, pd.Grouper(key=time_col, freq="D")])[
        "Steps"
    ].sum()

    # Find the <pids, days> to drop
    days_to_drop = total_steps_day[total_steps_day <= min_steps].index

    # Temporarily reindex dataframe with <pid, day>
    df_tmp = df.set_index([pid_col, df[time_col].dt.floor("D")])

    # Return only the <pids, days> that are not in the list to drop
    df_tmp = df_tmp.loc[~df_tmp.index.isin(days_to_drop)]

    # The first level of index (pid) needs to come back to the dataframe, but the second one (day) have to be dropped
    return df_tmp.reset_index(level=0).reset_index(drop=True)


# %%

# %% slideshow={"slide_type": "skip"}
# Check that days per patient are >= 3
days_per_patient = SequenceOperator(
    [
        GroupbyOperator(by="patientID"),
        ResampleOperator(
            "D", on="dateTime", aggregation_definition="size"
        ),  # Counts number of rows
        ResetIndexOperator(),
        AggregateOperator(
            groupby_feature_names=["patientID"], aggregation_definition={"": ["size"]}
        ),
    ]
)

days_per_patient = days_per_patient.process(df_intra)[0]
days_per_patient

# %% slideshow={"slide_type": "skip"}
df_intra[df_intra.patientID == 85]

# %% slideshow={"slide_type": "skip"}
good_avail_pipeline = SequenceOperator(
    [
        SortOperator(by=["patientID", "dateTime"]),
        GroupbyOperator(by=["patientID", "Ramadan"]),
        ApplyOperator(
            func=lambda g: (g["dateTime"].iloc[-1] - g["dateTime"].iloc[0]).days
        ),
        UnstackOperator(),
        FillNAOperator(value=0),
        RenameOperator(columns={0: "before", 1: "during"}),
        CreateFeatureOperator(
            feature_name="after",
            feature_creator=lambda _: 0,
        ),
        CreateFeatureOperator(
            feature_name="total",
            feature_creator=lambda df: df["before"] + df["during"] + df["after"],
        ),
        FilterOperator(
            epoch_filter=lambda df: (df["before"] >= 5) & (df["during"] >= 5)
        ),
    ]
)

good_avail_pipeline.process(df_intra)[0]

# %% slideshow={"slide_type": "skip"}
# Do we create report using dataprep operator?
df_ramadan_pipeline = ProcessingPipeline(
    [
        FilterOperator(epoch_filter=lambda df: df["Ramadan"] == 0),
    ]
)

df_ramadan = df_ramadan_pipeline.process(df_intra)[0]

create_report(
    df_ramadan[["patientID", "HeartRate", "Calories", "Steps", "Distance", "CGM"]]
)


# %% slideshow={"slide_type": "skip"}
import matplotlib.pyplot as plt


def boxplot_correlation(
    df,
    secondary_col,
    main_col="CGM",
    pid_col="patientID",
    time_col="time",
    remove_zero_steps=True,
):
    # Get day correlation
    pipeline = ProcessingPipeline(
        [
            CreateFeatureOperator(
                feature_name="Date", feature_creator=lambda df: df["dateTime"].date()
            ),
            GroupbyOperator(by=["patientID", "Date"]),
            CorrOperator(),
            ResetIndexOperator(),
            #         ApplyOperator(func=lambda df: df[df["level_2"] == main_col]),
            CreateFeatureOperator(
                feature_name="Ramadan",
                feature_creator=lambda df, fun=ramadan_flag: fun(df["Date"]),
            ),
        ]
    )

    df_tmp = df
    df_tmp = pipeline.process(df_tmp)[0]

    # Plot the orbital period with horizontal boxes
    sns.set_theme(style="ticks")
    plt.figure(figsize=(16, 6), dpi=80)
    plot = sns.boxplot(x="patientID", y=secondary_col, hue="Ramadan", data=df_tmp)
    sns.stripplot(
        x="patientID", y=secondary_col, data=df_tmp, size=4, color=".3", linewidth=0
    )

    plot.set(ylabel="Pearson Correlation\n%s-%s" % (main_col, secondary_col))
    sns.despine(offset=10, trim=True)

    # Move the legend to the right side
    plot.legend(title="Ramadan?", bbox_to_anchor=(1.3, 0.5), ncol=1)
    return plot


# %% slideshow={"slide_type": "skip"}
plot = boxplot_correlation(df_intra, "Steps", time_col="dateTime")
plt.show()

# %% slideshow={"slide_type": "skip"}
# Apply Operator


df_emr = df[0]
df_emr

emr_pipeline = SequenceOperator(
    [
        DropDuplicatesOperator(subset=["patientID", "variable"]),
        PivotOperator(index="patientID", columns="variable", values="value"),
        CreateFeatureOperator(
            feature_name="Diabetes Duration",
            feature_creator=lambda df: np.nan
            if df["Diabetes Duration"] == ""
            else df["Diabetes Duration"],
        ),
        AsTypeOperator(
            {
                "BMI": float,
                "Cholesterol": float,
                "Creatinine": float,
                "Diabetes Duration": float,
                "Diastolic Blood Pressure": float,
                "HDL": float,
                "HbA1c": float,
                "LDL": float,
                "Systolic Blood Pressure": float,
            }
        ),
        CreateFeatureOperator(
            feature_name="Diabetes Medication",
            feature_creator=lambda df: df["Diabetes Medication"]
            .replace("+", ", ")
            .split(","),
        ),
        CreateFeatureOperator(
            feature_name="Diabetes Medication",
            feature_creator=lambda df: [
                x.split()[0].strip() for x in df["Diabetes Medication"] if x.strip()
            ],
        ),
    ]
)


df_emr = emr_pipeline.process(df_emr)[0]
df_emr

# %% [markdown] slideshow={"slide_type": "subslide"}
# # Quick statistics

# %% slideshow={"slide_type": "subslide"}
import numpy as np

# one aggregator
# Concatenation should be simpler

df_agg_pipeline = AggregateOperator(
    groupby_feature_names=["patientID", pd.Grouper(key="dateTime", freq="D")],
    aggregation_definition={
        "Steps": ["sum", "mean"],
        "Calories": ["sum", "mean"],
        "Distance": ["sum", "mean"],
        "HeartRate": ["mean"],
        "CGM": ["mean"],
        "hypo": ["mean", "sum"],
        "hyper": ["mean", "sum"],
        "Ramadan": ["mean"],
    },
)


# Concatenate columns only
df_intra_result = df_agg_pipeline.process(df_intra)
df_intra_result = df_intra_result[0]
df_intra_result


# %% slideshow={"slide_type": "subslide"}
# Original code had to define multiple functions in order to calculate some statistics


def agg_per_day(
    df,
    metric,
    operation,
    outputcol_name=None,
    pid_col="patientID",
    time_col="time",
    remove_zero_steps=False,
):

    if remove_zero_steps:
        df_tmp = df[df["Steps"] > 0]
    else:
        df_tmp = df.copy()

    # Get aggregated number of [steps, calories, ...] in a day
    metric_per_day = df_tmp.groupby([pid_col, pd.Grouper(key=time_col, freq="D")])[
        metric
    ].agg(operation)
    if outputcol_name is not None:
        metric_per_day.name = outputcol_name
    return metric_per_day


def sum_per_day(
    df,
    metric,
    outputcol_name=None,
    pid_col="patientID",
    time_col="time",
    remove_zero_steps=False,
):
    return agg_per_day(
        df, metric, "sum", outputcol_name, pid_col, time_col, remove_zero_steps
    )


def mean_per_day(
    df,
    metric,
    outputcol_name=None,
    pid_col="patientID",
    time_col="time",
    remove_zero_steps=False,
):
    return agg_per_day(
        df, metric, "mean", outputcol_name, pid_col, time_col, remove_zero_steps
    )


df_agg = pd.concat(
    [
        mean_per_day(df_intra, "Steps", "MeanSteps", remove_zero_steps=True),
        sum_per_day(df_intra, "Steps", "SumSteps", remove_zero_steps=True),
        mean_per_day(df_intra, "Calories", remove_zero_steps=True),
        sum_per_day(df_intra, "Calories", "SumCalories", remove_zero_steps=True),
        mean_per_day(df_intra, "Distance", remove_zero_steps=True),
        sum_per_day(df_intra, "Distance", "SumDistance", remove_zero_steps=True),
        mean_per_day(df_intra, "mets", remove_zero_steps=True),
        mean_per_day(df_intra, "HeartRate", remove_zero_steps=True),
        mean_per_day(df_intra, "CGM", remove_zero_steps=True),
        mean_per_day(df_intra, "hypo", "AvgHypo", remove_zero_steps=True),
        sum_per_day(df_intra, "hypo", "TotalHypo", remove_zero_steps=True),
        mean_per_day(df_intra, "hyper", "AvgHyper", remove_zero_steps=True),
        sum_per_day(df_intra, "hyper", "TotalHyper", remove_zero_steps=True),
        mean_per_day(df_intra, "Ramadan", "IsRamadan", remove_zero_steps=True),
    ],
    axis=1,
).reset_index()
df_means = df_agg.groupby("patientID").mean()
df_means.head()

# %% [markdown] slideshow={"slide_type": "subslide"}
# # Create custom time-related features from our dataframe
#
# Inputting time in seconds is not a useful model feature. It has clear daily and yearly periodicity. There are many ways to deal with periodicity.
#
# ![output_mXBbTJZfuuTC_1.png](attachment:output_mXBbTJZfuuTC_1.png)

# %% slideshow={"slide_type": "subslide"}
# Time cyclical encoding
# category_definition can be 'month', 'day', 'hour'
df_intra = EncodeCyclicalFeaturesOperator(
    date_feature_name="dateTime", category_definition="hour"
).process(df_intra)[0]

df_intra[["dateTime", "hour_sin", "hour_cos"]]


# %% slideshow={"slide_type": "subslide"}
# Original code


def convert_time_sin_cos(df, datetime_col):

    day = 24 * 60 * 60

    ts = df[datetime_col].apply(lambda x: x.timestamp()).astype(int)
    day_sin = np.sin(ts * (2 * np.pi / day))
    day_cos = np.cos(ts * (2 * np.pi / day))

    return day_sin, day_cos


# %% [markdown] slideshow={"slide_type": "subslide"}
# # Or, we may call popular timeseries-related packages to extract the features from our dataset
# - Kats
# - Tsfresh
# - However, these packages require converting our dataset into sequences

# %% [markdown] slideshow={"slide_type": "subslide"}
#
# # To create the sequences, we use a sliding window method.
#
# ![Capture.PNG](attachment:Capture.PNG)

# %% slideshow={"slide_type": "subslide"}
# Create timeseries dataset X, y through a sliding window
tsfresh_input = SlidingWindowOperator(
    winsize="3h15t",
    time_col="dateTime",
    label_col="CGM",
    participant_identifier="patientID",
).process(df_intra)

tsfresh_input[0][0][["dateTime", "CGM", "seq_id"]]

# %% [markdown] slideshow={"slide_type": "subslide"}
# # Extract sequence features using TSFresh package
#
#
# <div>
# <img src="attachment:Capture2-2.PNG" width="600"/>
# </div>
#

# %% slideshow={"slide_type": "subslide"}

tsfresh_features = TSFreshFeatureExtractorOperator(
    seq_id_col="seq_id", time_col="dateTime", value_col="Steps"
).process(tsfresh_input[0][0])

# SeqID * Features
tsfresh_features[0].dropna(axis=1)

# %% [markdown] slideshow={"slide_type": "subslide"}
# # Extract timeseries properties using Kats package
#
# <br></br>
#
# <div>
# <img src="attachment:kats_logo.svg" width="600"/>![kats_logo.svg](attachment:kats_logo.svg)
# </div>
#

# %% slideshow={"slide_type": "skip"}
kats_input = tsfresh_input[0][0]
kats_input = kats_input[kats_input.seq_id.isin([0, 1, 2])]
kats_input

# %% slideshow={"slide_type": "subslide"}
kats_features = CalculateTimeseriesPropertiesOperator(
    timeseries_column="dateTime", value_column="Steps"
).process(kats_input)

kats_features
