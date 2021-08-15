"""
Module that provides classes to access sleep health datasets.
For details please read https://www.nature.com/articles/s41597-020-00753-2

Datasets Included in this module

One Time Questionnaires:
    - Onboarding Demographics
    - About Me
    - My Family
    - My Health
    - Research Interest
    - Sleep Assessment
    - Sleep Habits

Recurrent Questionnaires:
    - Nap Tracker
    - AM Check-in
    - PM Check-in
    - Sleep Quality Checker
    - Sleepiness Checker
    - Alertness Checker - Psychomotor Vigilance Task

Wearable Data:
    - HealthKit Data

"""
from tasrif.processing_pipeline.pandas import ReadCsvOperator


class SleepHealthDataset(ReadCsvOperator):
    """Base class for all Sleep fitbit datasets
    """
