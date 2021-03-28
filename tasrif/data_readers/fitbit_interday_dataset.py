"""Module that provides classes to work with a exported fitbit dataset at interday resolution
"""

import pathlib
from io import StringIO
import pandas as pd
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.pandas import JsonNormalizeOperator, ConvertToDatetimeOperator,\
                                              SetIndexOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator, AggregateOperator

class FitbitInterdayDataset:
    """Base class for all fitbit interday datasets
    """
    class Default: #pylint: disable=too-few-public-methods
        """Default processing pipeline to use. Must be set in subclass.
        """
        PIPELINE = None # Set this in subclass

    table_name = None # Set this in subclass

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing interday data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        # Accumulate dataframes from all files and concat them all at once
        dfs = []
        for export_file in pathlib.Path(self.folder).glob('fitbit_export_*.csv'):
            dfs.append(self._extract_data_from_table(export_file))
        self.raw_df = pd.concat(dfs)

        self._process()

    def _process(self):
        self.processed_df = (self.processing_pipeline.process(self.raw_df))[0]

    def _extract_data_from_table(self, file_name):
        """Extracts the table data from the interday dump. Requires 'table_name' attribute to be set.

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object corresponding to the raw data.
        """
        found = False
        table_start_line = None
        table_end_line = None
        with open(file_name, "r") as file_object:
            # Iterate through stacked csv and get the start and end of the table
            for line_number, line in enumerate(file_object):
                if line.strip() == self.table_name:
                    # The start of the table has been found.
                    found = True
                    table_start_line = line_number + 1 # The data starts from the next line.
                    continue
                if found:
                    # If the line is empty, we've hit the end of the table
                    if line.strip() == "":
                        table_end_line = line_number
                        break

            # Go back to the start of the file
            file_object.seek(0)
            # Force pandas to skip rows that don't contain the table
            return pd.read_csv(
                file_object,
                skiprows=lambda l: not (table_start_line <= l < table_end_line),
                thousands=','
            )

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):#pylint: disable=no-self-use
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class FitbitBodyDataset(FitbitInterdayDataset):
    """Class to work with body measurement data from a fitbit export dump.
    """
    class Default: #pylint: disable=too-few-public-methods
        """The default pipeline consists of the following high-level steps:
        - converts Date field to DateTime
        - set index to Date field
        """
        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=['Date'], infer_datetime_format=True),
            SetIndexOperator('Date')
        ])

    table_name = "Body"

class FitbitFoodsDataset(FitbitInterdayDataset):
    """Class to work with food (calorie intake) data from a fitbit export dump.
    """
    class Default: #pylint: disable=too-few-public-methods
        """The default pipeline consists of the following high-level steps:
        - converts Date field to DateTime
        - set index to Date field
        """
        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=['Date'], infer_datetime_format=True),
            SetIndexOperator('Date')
        ])

    table_name = "Foods"

class FitbitActivitiesDataset(FitbitInterdayDataset):
    """Class to work with physical activities data from a fitbit export dump.
    """
    class Default: #pylint: disable=too-few-public-methods
        """The default pipeline consists of the following high-level steps:
        - converts Date field to DateTime
        - set index to Date field
        """
        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=['Date'], infer_datetime_format=True),
            SetIndexOperator('Date')
        ])

    table_name = "Activities"

class FitbitSleepDataset(FitbitInterdayDataset):
    """Class to work with sleep data from a fitbit export dump.
    """
    class Default: #pylint: disable=too-few-public-methods
        """The default pipeline consists of the following high-level steps:
        - converts 'Start Time' and 'End Time' fields to DateTime
        - copies 'End Time' into a new field, Date
        - groups rows by the Date field and sums up features
        - set index to Date field
        """
        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=['Start Time', 'End Time'], infer_datetime_format=True),
            CreateFeatureOperator(feature_name="Date", feature_creator=lambda df: df['End Time'].date()),
            AggregateOperator(groupby_feature_names="Date", aggregation_definition={
                'Minutes Asleep': 'sum',
                'Minutes Awake': 'sum',
                'Number of Awakenings': 'sum',
                'Time in Bed': 'sum',
                'Minutes REM Sleep': 'sum',
                'Minutes Light Sleep': 'sum',
                'Minutes Deep Sleep': 'sum'
            }),
            SetIndexOperator('Date')
        ])

    table_name = "Sleep"

class FitbitFoodLogDataset(FitbitInterdayDataset):
    """Class to work with food log data from a fitbit export dump.
    """

    class Default: #pylint: disable=too-few-public-methods
        """The default pipeline consists of the following high-level steps:
        - json_normalize
        - converts Date field to DateTime
        - set index to Date field
        """
        PIPELINE = ProcessingPipeline([
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["Date"], infer_datetime_format=True),
            SetIndexOperator('Date')
        ])

    def _extract_data_from_table(self, file_name):
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

                    next(file_object) # Skip "Daily Totals" line

                    # Grab all the measurements from the entry
                    for _ in range(num_measurements_in_entry):
                        measurement_line = next(file_object)
                        _, measurement, value = pd.read_csv(StringIO(measurement_line), thousands=',')
                        value = value.split(" ")[0] # Get rid of the measurement unit, if any
                        food_log_entry[measurement.strip()] = value.strip()

                    # Collect entries into dataframe
                    food_log_entries.append(food_log_entry)

        return food_log_entries
