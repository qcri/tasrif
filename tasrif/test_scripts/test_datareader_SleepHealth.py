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
from tasrif.data_readers.sleep_health import SleepHealthDataset
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    DropNAOperator,
    DropFeaturesOperator,
    DropDuplicatesOperator,
    ReplaceOperator,
    SortOperator
)
from tasrif.processing_pipeline.custom import (
    CreateFeatureOperator,
    AggregateOperator,
    EncodeCyclicalFeaturesOperator,
    OneHotEncoderOperator
)

sleephealth_path = os.environ['SLEEPHEALTH']

pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "aboutme"),
    DropNAOperator(subset=[
        "alcohol",
        "basic_expenses",
        "caffeine",
        "daily_activities",
        "daily_smoking",
        "education",
        "flexible_work_hours",
        "gender",
        "good_life",
        "hispanic",
        "income",
        "race",
        "work_schedule",
        "weight",
        "smoking_status",
        "marital",
    ]),
    DropDuplicatesOperator(subset="participantId", keep="last"),
])

df = pipeline.process()

# %% pycharm={"name": "#%%\n"}
pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "amcheckin"),
    ConvertToDatetimeOperator(feature_names=["AMCH1", "AMCH4"],
                              format="%Y-%m-%dT%H:%M:%S%z",
                              utc=True),
    SortOperator(by=["participantId"]),
    ReplaceOperator(
        to_replace={
            "AMCH2A": {
                np.nan: 0
            },
            "AMCH3A": {
                np.nan: 0
            },
            "AMCH5": {
                np.nan: 0
            },
        }),
    DropNAOperator(
        subset=["participantId", "AMCH1", "AMCH2", "AMCH3", "AMCH4"]),
    DropFeaturesOperator(['participantId', 'timestamp']),
    EncodeCyclicalFeaturesOperator(date_feature_name='AMCH4',
                                   category_definition=[
                                       "month", "day_in_month", "day",
                                       "hour", "minute"
                                   ])
])


df = pipeline.process()

# %% pycharm={"name": "#%%\n"}
pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "sleephabits"),
    ConvertToDatetimeOperator(feature_names=["timestamp"],
                              format="%Y-%m-%dT%H:%M:%S",
                              utc=True),
    ReplaceOperator(
        to_replace={
            "driving_sleepy": {
                6: np.nan
            },
            "morning_person": {
                3: np.nan
            },
            "nap_duration": {
                6: np.nan
            },
            "what_wakes_you": {
                13: np.nan
            },
        }),
    DropNAOperator(subset=[
        "alarm_dependency",
        "driving_sleepy",
        "falling_asleep",
        "sleep_needed",
        "sleep_partner",
        "sleep_time_workday",
        "wake_up_choices",
        "wake_ups",
        "what_wakes_you",
    ]),
    OneHotEncoderOperator(feature_names=[
        "alarm_dependency",
        "driving_sleepy",
        "falling_asleep",
        "morning_person",
        "nap_duration",
        "sleep_partner",
        "wake_up_choices",
        "weekly_naps",
        "what_wakes_you",
    ]),
])

# %% pycharm={"name": "#%%\n"}

pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "myfamily"),
    ConvertToDatetimeOperator(feature_names="timestamp",
                              format="%Y-%m-%dT%H:%M:%S%z",
                              utc=True),
    SortOperator(by=["participantId", "timestamp"]),
    DropDuplicatesOperator(subset="participantId", keep="last"),
    ReplaceOperator(
        to_replace={
            "fam_history": {
                "200": np.nan
            },
            "family_size": {
                6: np.nan
            },
            "language": {
                5: np.nan
            },
            "underage_family": {
                6: np.nan
            },
        }),
    DropNAOperator(subset=[
        "fam_history", "family_size", "language", "underage_family"
    ]),
    OneHotEncoderOperator(
        feature_names=[
            "fam_history",
            "family_size",
            "language",
            "underage_family",
        ],
        drop_first=False,
    ),
    DropFeaturesOperator(["fam_history=200"]),
])

df = pipeline.process()

# %% pycharm={"name": "#%%\n"}
pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "myhealth"),
    ConvertToDatetimeOperator(feature_names="timestamp",
                              format="%Y-%m-%dT%H:%M:%S%z",
                              utc=True),
    SortOperator(by=["participantId", "timestamp"]),
    DropDuplicatesOperator(subset="participantId", keep="last"),
    ReplaceOperator(
        to_replace={
            "allergies": {
                3: np.nan
            },
            "anxiety": {
                3: np.nan
            },
            "apnea": {
                3: np.nan
            },
            "asthma": {
                3: np.nan
            },
            "atrial": {
                3: np.nan
            },
            "hi_blood_pressure": {
                3: np.nan
            },
            "cancer": {
                3: np.nan
            },
            "depression": {
                3: np.nan
            },
            "diabetes": {
                3: np.nan
            },
            "erectile": {
                3: np.nan
            },
            "gastroesophageal": {
                3: np.nan
            },
            "heart_disease": {
                3: np.nan
            },
            "insomnia": {
                3: np.nan
            },
            "lung": {
                3: np.nan
            },
            "narcolepsy": {
                3: np.nan
            },
            "nocturia": {
                3: np.nan
            },
            "restless_legs_syndrome": {
                3: np.nan
            },
            "stroke": {
                3: np.nan
            },
            "uars": {
                3: np.nan
            },
        }),
    DropNAOperator(subset=[
        "anxious",
        "cardiovascular",
        "compare_one_year",
        "day_to_day",
        "depressed",
        "emotional",
        "fatigued",
        "general_health",
        "mental_health",
        "physical_activities",
        "physical_health",
        "risk",
        "sleep_trouble",
        "social_activities",
        "stressed",
    ]),
    OneHotEncoderOperator(
        feature_names=[
            "anxious",
            "cardiovascular",
            "compare_one_year",
            "day_to_day",
            "depressed",
            "emotional",
            "fatigued",
            "general_health",
            "mental_health",
            "physical_activities",
            "physical_health",
            "risk",
            "sleep_trouble",
            "social_activities",
            "stressed",
        ],
        drop_first=True,
    ),
])

df = pipeline.process()
# %% pycharm={"name": "#%%\n"}
pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "onboardingdemographics"),
    ReplaceOperator(to_replace="CENSORED", value=np.nan),
    DropNAOperator()
])

df = pipeline.process()

# %% pycharm={"name": "#%%\n"}

pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "pmcheckin"),
    ConvertToDatetimeOperator(feature_names=["timestamp"],
                              format="%Y-%m-%dT%H:%M:%S%z",
                              utc=True),
    SortOperator(by=["participantId", "timestamp"]),
    ReplaceOperator(to_replace={
        "PMCH2A": {
            np.nan: 0
        },
        "PMCH3": {
            np.nan: 0
        },
        "PMCH1": {
            np.nan: 3
        },
        "NapCount": {
            np.nan: 0
        },
    }),
    DropFeaturesOperator(['participantId', 'timestamp']),
    OneHotEncoderOperator(feature_names=['NapCount', 'PMCH1', 'PMCH3'],
                          drop_first=False)
])


df = pipeline.process()

# %% pycharm={"name": "#%%\n"}
pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "sleepqualitychecker"),
    SortOperator(by=["participantId", "timestamp"]),
    AggregateOperator(
        groupby_feature_names="participantId",
        aggregation_definition={
            "sq_score": [
                "count",
                "mean",
                "std",
                "min",
                "max",
                "first",
                "last",
            ],
            "timestamp": ["first", "last"],
        },
    ),
    ConvertToDatetimeOperator(
        feature_names=["timestamp_last", "timestamp_first"],
        format="%Y-%m-%dT%H:%M:%S%z",
        utc=True,
    ),
    CreateFeatureOperator(
        feature_name="delta_first_last_timestamp",
        feature_creator=lambda row: row["timestamp_last"] - row[
            "timestamp_first"],
    ),
])

df = pipeline.process()

# %% pycharm={"name": "#%%\n"}
pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "researchinterest"),
    ConvertToDatetimeOperator(feature_names="timestamp",
                              format="%Y-%m-%dT%H:%M:%S%z",
                              utc=True),
    SortOperator(by=["participantId", "timestamp"]),
    DropDuplicatesOperator(subset="participantId", keep="last"),
    ReplaceOperator(to_replace={"research_experience": {
        3: np.nan
    }}),
    DropNAOperator(subset=[
        "contact_method",
        "research_experience",
        "two_surveys_perday",
        "blood_sample",
        "taking_medication",
        "family_survey",
        "hospital_stay",
    ]),
    OneHotEncoderOperator(
        feature_names=[
            "contact_method",
            "research_experience",
            "two_surveys_perday",
            "blood_sample",
            "taking_medication",
            "family_survey",
            "hospital_stay",
        ],
        drop_first=True,
    ),
])

df = pipeline.process()

# %% pycharm={"name": "#%%\n"}
pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "sleepassessment"),
    ConvertToDatetimeOperator(feature_names="timestamp",
                              format="%Y-%m-%dT%H:%M:%S%z",
                              utc=True),
    SortOperator(by=["participantId", "timestamp"]),
    DropDuplicatesOperator(subset="participantId", keep="last"),
    ReplaceOperator(
        to_replace={
            "alcohol": {
                7: np.nan
            },
            "medication_by_doctor": {
                7: np.nan
            },
            "sleep_aids": {
                7: np.nan
            },
            "told_by_doctor": {
                3: np.nan
            },
            "told_to_doctor": {
                3: np.nan
            },
            "told_by_doctor_specify": {
                np.nan: "8"
            },
            "other_selected": {
                np.nan: ""
            },
        }),
    DropNAOperator(subset=[
        "alcohol",
        "concentrating_problem_one",
        "concentrating_problem_two",
        "discomfort_in_sleep",
        "exercise",
        "fatigue_limit",
        "feel_tired_frequency",
        "felt_alert",
        "had_problem",
        "hard_times",
        "medication_by_doctor",
        "poor_sleep_problems",
        "sleep_aids",
        "sleep_problem",
        "think_clearly",
        "tired_easily",
        "told_by_doctor",
        "told_to_doctor",
        "trouble_staying_awake",
    ]),
    OneHotEncoderOperator(
        feature_names=[
            "alcohol",
            "concentrating_problem_one",
            "concentrating_problem_two",
            "discomfort_in_sleep",
            "exercise",
            "fatigue_limit",
            "feel_tired_frequency",
            "felt_alert",
            "had_problem",
            "hard_times",
            "medication_by_doctor",
            "poor_sleep_problems",
            "sleep_aids",
            "sleep_problem",
            "think_clearly",
            "tired_easily",
            "told_by_doctor",
            "told_to_doctor",
            "trouble_staying_awake",
            "told_by_doctor_specify",
        ],
        drop_first=True,
    ),
])


df = pipeline.process()
