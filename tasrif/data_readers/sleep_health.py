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
import pathlib
import pandas as pd
from tasrif.processing_pipeline import ProcessingOperator


class SleepHealthDataset(ProcessingOperator):
    """Base class for all Sleep fitbit datasets
    """

    valid_table_names = ["aboutme", "amcheckin", "myfamily", "myhealth", "onboardingdemographics", "pmcheckin",
                         "researchinterest", "sleepassessment", "sleephabits", "sleepqualitychecker"]

    def __init__(self, file_name, table_name):
        """Initializes a dataset reader with the input parameters.

        Args:
            file_name (str):
                Path to the withings export file containing data.
            table_name (str):
                The table to extract data from.

        """
        # Abort if table_name isn't valid
        super().__init__()
        self._validate_table_name(table_name)

        self.path_name = file_name
        self.table_name = table_name

    def process(self, *data_frames):
        if self.table_name == "aboutme":
            path = pathlib.Path(self.path_name, "About Me.csv")
        elif self.table_name == "amcheckin":
            path = pathlib.Path(self.path_name, "AM Check-in.csv")
        elif self.table_name == "myfamily":
            path = pathlib.Path(self.path_name, "My Family.csv")
        elif self.table_name == "myhealth":
            path = pathlib.Path(self.path_name, "My Health.csv")
        elif self.table_name == "onboardingdemographics":
            path = pathlib.Path(self.path_name, "Onboarding Demographics.csv")
        elif self.table_name == "pmcheckin":
            path = pathlib.Path(self.path_name, "PM Check-in.csv")
        elif self.table_name == "researchinterest":
            path = pathlib.Path(self.path_name, "Research Interest.csv")
        elif self.table_name == "sleepassessment":
            path = pathlib.Path(self.path_name, "Sleep Assessment.csv")
        elif self.table_name == "sleephabits":
            path = pathlib.Path(self.path_name, "Sleep Habits.csv")
        elif self.table_name == "sleepqualitychecker":
            path = pathlib.Path(self.path_name, "Sleep Quality Checker.csv")

        return [pd.read_csv(path)]

    def _validate_table_name(self, table_name):
        if table_name not in self.valid_table_names:
            raise RuntimeError(
                f"Invalid table_name, must be from the following: {self.valid_table_names}")
