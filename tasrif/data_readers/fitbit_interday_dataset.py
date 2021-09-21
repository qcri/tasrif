"""Module that provides classes to work with a exported fitbit dataset at interday resolution
"""

import pathlib
from io import StringIO
import pandas as pd
from tasrif.processing_pipeline import ProcessingOperator

class FitbitInterdayDataset(ProcessingOperator):
    """Base class for all fitbit interday datasets
    """
    valid_table_names = ["Activities", "Body", "Food Log", "Foods", "Sleep"]

    def __init__(self, folder_path, table_name):
        """Initializes an interday dataset reader with the input parameters.

        Args:
            folder_path (str):
                Path to the fitbit export folder_path containing interday data.
            table_name (str):
                The table to extract data from.
        """
        # Abort if table_name isn't valid
        super().__init__()
        self._validate_table_name(table_name)

        self.folder_path = folder_path
        self.table_name = table_name

    def process(self, *data_frames):
        # Accumulate dataframes from all files and concat them all at once
        dfs = []
        for export_file in pathlib.Path(
                self.folder_path).glob('fitbit_export_*.csv'):
            dfs.append(self._extract_data_from_file(export_file))
        return [pd.concat(dfs)]

    def _validate_table_name(self, table_name):
        if table_name not in self.valid_table_names:
            raise RuntimeError(f"Invalid table_name, must be from the following: {self.valid_table_names}")

    def _extract_data_from_file(self, file_name):
        """Extracts the table data from the interday dump. Requires 'table_name' attribute to be set.

        Args:
            file_name (str):
                name of the file

        Returns:
            pd.Dataframe
                Pandas dataframe object corresponding to the raw data.
        """
        if self.table_name == "Food Log":
            return self._extract_food_log_table(file_name)

        found = False
        table_start_line = None
        table_end_line = None
        with open(file_name, "r") as file_object:
            # Iterate through stacked csv and get the start and end of the table
            for line_number, line in enumerate(file_object):
                if line.strip() == self.table_name:
                    # The start of the table has been found.
                    found = True
                    table_start_line = line_number + 1  # The data starts from the next line.
                    continue
                if found:
                    # If the line is empty, we've hit the end of the table
                    if line.strip() == "":
                        table_end_line = line_number
                        break

            # Go back to the start of the file
            file_object.seek(0)
            # Force pandas to skip rows that don't contain the table
            return pd.read_csv(file_object,
                            skiprows=lambda l: not (table_start_line <= l <
                                                    table_end_line),
                            thousands=',')

    @staticmethod
    def _extract_food_log_table(file_name):
        """
        Food Log entries require vastly different parsing compared to the other datasets.
        Example entry from csv:
            Food Log 20190701
            Daily Totals
            "","Calories","0"
            "","Fat","0 g"
            "","Fiber","0 g"
            "","Carbs","0 g"
            "","Sodium","0 mg"
            "","Protein","0 g"
            "","Water","0 ml"

        Args:
            file_name (str):
                name of the file

        Returns:
            pd.DataFrame
        """
        num_measurements_in_entry = 7
        food_log_entries = []

        with open(file_name, "r") as file_object:
            for line in file_object:
                if line.startswith("Food Log"):
                    # Capture data belonging to a single food log entry into a dict
                    food_log_entry = {}

                    # Grab date from table heading
                    date = line.split(" ")[2].strip()
                    food_log_entry['Date'] = date

                    next(file_object)  # Skip "Daily Totals" line

                    # Grab all the measurements from the entry
                    for _ in range(num_measurements_in_entry):
                        measurement_line = next(file_object)
                        _, measurement, value = pd.read_csv(
                            StringIO(measurement_line),
                            thousands=','
                        )
                        # Get rid of the measurement unit, if any
                        value = value.split(" ")[0]
                        food_log_entry[measurement.strip()] = value.strip()

                    # Collect entries into dataframe
                    food_log_entries.append(food_log_entry)

        # Convert entries into a DataFrame before returning
        return pd.DataFrame(food_log_entries)
