"""
Module that provides classes to work with a exported fitbit dataset at intraday resolution

Available datasets
------------------
    - Nutrition (food and water logs)
    - Weight
    - Physical Activity (distance, calories, heart-rate, active and sedentary minutes)
    - Sleep
"""

import pathlib
import json
from tasrif.processing_pipeline import ProcessingOperator


class FitbitIntradayDataset(ProcessingOperator):
    """Base class for all Fitbit Intraday datasets
    """
    valid_table_names = ["Sleep", "Calories", "Distance", "Steps", "Time_in_Heart_Rate_Zones", "Heart_Rate",
                         "Very_Active_Minutes", "Lightly_Active_Minutes", "Moderately_Active_Minutes",
                         "Sedentary_Minutes"]

    def __init__(self, folder_path, table_name):
        """Initializes a dataset reader with the input parameters.

        Args:
            folder_path (str):
                Path to the Fitbit Intraday folder_path containing data.
            table_name (str):
                The table to extract data from.

        """
        # Abort if table_name isn't valid
        super().__init__()
        self._validate_table_name(table_name)

        self.folder_path = folder_path
        self.table_name = table_name

    def process(self, *data_frames):

        data_frame = []

        if self.table_name in ["Sleep"]:
            full_path = pathlib.Path(self.folder_path, "Sleep")
        else:
            full_path = pathlib.Path(self.folder_path, "Physical Activity")

        file_paths = []
        for file_path in pathlib.Path(str(full_path)).glob(self.table_name.lower() + "*.json"):
            file_paths.append(str(file_path))

        for file_path in file_paths:
            with open(file_path) as json_file:
                json_data = json.load(json_file)

                data_frame.extend(json_data)

        return [data_frame]

    def _validate_table_name(self, table_name):
        if table_name not in self.valid_table_names:
            raise RuntimeError(
                f"Invalid table_name, must be from the following: {self.valid_table_names}")
