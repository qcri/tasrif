"""
Module that provides classes to work with a fitbit dataset dumped from SIHA
    **Available datasets**:
        - EMR,
        - CGM,
        - Sleep, SleepIntraday
        - Steps, StepsIntraday
        - Distance, DistanceIntraday
        - Calories, CaloriesIntraday
        - HeartRateIntraday
        - TimeInHeartRateZones, SedentaryActiveMinutes, LightlyActiveMinutes, ModerateActiveMinutes, VeryActiveMinutes
"""

import pathlib
import json

from tasrif.processing_pipeline import (
    ProcessingOperator,
)


class SihaDataset(ProcessingOperator):
    """Base class to work with the all SIHA based datasets."""

    valid_table_names = [
        "EMR",
        "CGM",
        "Sleep",
        "Steps",
        "Distance",
        "Calories",
        "SleepIntraday",
        "StepsIntraday",
        "DistanceIntraday",
        "CaloriesIntraday",
        "HeartRateIntraday",
        "TimeInHeartRateZones",
        "SedentaryActiveMinutes",
        "LightlyActiveMinutes",
        "ModerateActiveMinutes",
        "VeryActiveMinutes",
    ]

    def __init__(self, folder, table_name):
        """Initializes a dataset reader with the input parameters.

        Args:
            folder (str):
                Path to the SIHA export folder containing data.
            table_name (str):
                The table to extract data from.
        """
        # Abort if table_name isn't valid
        self._validate_table_name(table_name)

        self.folder = folder
        self.table_name = table_name

    def _validate_table_name(self, table_name):
        if table_name not in self.valid_table_names:
            raise RuntimeError(f"Invalid table_name, must be from the following: {self.valid_table_names}")

    def process(self, *data_frames):

        jsons = []
        for file_path in pathlib.Path(self.folder).glob("data*.json"):
            with open(str(file_path)) as json_file:
                jsons.append(json.load(json_file))

        return jsons
