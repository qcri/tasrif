"""
Module that provides class to work with the MyHeartCounts dataset.
"""
import pathlib
import pandas as pd
from tasrif.processing_pipeline import ProcessingOperator


class MyHeartCountsDataset(ProcessingOperator):
    """
    Class to work with the MyHeartCounts dataset.
    """
    day_one_survey_device_mapping = {
        "iPhone": "1",
        "ActivityBand": "2",
        "Pedometer": "2",
        "SmartWatch": "3",
        "AppleWatch": "3",
        "Other": "Other",
    }

    valid_table_names = ["activitysleepsurvey", "cardiodietsurvey", "dailychecksurvey", "dayonesurvey", "demographics",
                         "healthkitdata", "healthkitsleep", "heartagesurvey", "parqsurvey", "qualityoflife",
                         "riskfactorsurvey", "sixminutewalkactivity"]

    def __init__(self, path_name, table_name):
        """Initializes a dataset reader with the input parameters.

        Args:
            path_name (str):
                Path to the withings export file containing data.
            table_name (str):
                The table to extract data from.

        """
        # Abort if table_name isn't valid
        super().__init__()
        self._validate_table_name(table_name)

        self.path_name = path_name
        self.table_name = table_name

    def process(self, *data_frames):
        if self.table_name == "activitysleepsurvey":
            path = pathlib.Path(self.path_name, 'Activity and Sleep Survey.csv')
        elif self.table_name == "cardiodietsurvey":
            path = pathlib.Path(self.path_name, 'Cardio Diet Survey.csv')
        elif self.table_name == "dailychecksurvey":
            path = pathlib.Path(self.path_name, 'Daily Check Survey.csv')
        elif self.table_name == "dayonesurvey":
            path = pathlib.Path(self.path_name, 'Day One Survey.csv')
        elif self.table_name == "demographics":
            path = pathlib.Path(self.path_name, 'Demographics Survey.csv')
        elif self.table_name == "healthkitdata":
            path = pathlib.Path(self.path_name, 'HealthKit Data.csv')
        elif self.table_name == "healthkitsleep":
            path = pathlib.Path(self.path_name, 'HealthKit Sleep.csv')
        elif self.table_name == "heartagesurvey":
            path = pathlib.Path(self.path_name, 'APH Heart Age Survey.csv')
        elif self.table_name == "parqsurvey":
            path = pathlib.Path(self.path_name, 'PAR-Q Survey.csv')
        elif self.table_name == "qualityoflife":
            path = pathlib.Path(self.path_name, 'Satisfied Survey.csv')
        elif self.table_name == "riskfactorsurvey":
            path = pathlib.Path(self.path_name, 'Risk Factor Survey.csv')
        elif self.table_name == "sixminutewalkactivity":
            path = pathlib.Path(self.path_name, 'Six Minute Walk Activity.csv')

        return [pd.read_csv(path)]

    def _validate_table_name(self, table_name):
        if table_name not in self.valid_table_names:
            raise RuntimeError(
                f"Invalid table_name, must be from the following: {self.valid_table_names}")
