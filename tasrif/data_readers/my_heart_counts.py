"""
Module that provides class to work with the MyHeartCounts dataset.
"""
import pathlib
import warnings

import pandas as pd

from tasrif.processing_pipeline import ProcessingOperator, SequenceOperator
from tasrif.processing_pipeline.custom import (
    FilterOperator,
    ReadNestedCsvOperator,
    ReadNestedJsonOperator,
)
from tasrif.processing_pipeline.pandas import (
    AsTypeOperator,
    ConvertToDatetimeOperator,
    DropNAOperator,
    SortOperator,
)


class MyHeartCountsDataset(ProcessingOperator):  # pylint: disable=R0902
    """
    Class to work with the MyHeartCounts dataset.
    """

    class Defaults:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class."""

        CSV_PIPELINE = SequenceOperator(
            [
                ConvertToDatetimeOperator(["startTime", "endTime"], utc=True),
                DropNAOperator(),
                AsTypeOperator({"value": "float64"}),
                SortOperator(by="startTime"),
            ]
        )

    day_one_survey_device_mapping = {
        "iPhone": "1",
        "ActivityBand": "2",
        "Pedometer": "2",
        "SmartWatch": "3",
        "AppleWatch": "3",
        "Other": "Other",
    }

    valid_table_names = [
        "activitysleepsurvey",
        "cardiodietsurvey",
        "dailychecksurvey",
        "dayonesurvey",
        "demographics",
        "healthkitdata",
        "healthkitsleep",
        "heartagesurvey",
        "parqsurvey",
        "qualityoflife",
        "riskfactorsurvey",
        "sixminutewalkactivity",
    ]

    def __init__(  # pylint: disable=R0913
        self,
        path_name,
        table_name,
        participants=None,
        types=None,
        sources=None,
        split=False,
        nested_files_path=None,
        nested_files_pipeline=Defaults.CSV_PIPELINE,
    ):
        """Initializes a dataset reader with the input parameters.

        Args:
            path_name (str):
                Path to the myheartcounts file containing data.
            table_name (str):
                The table to extract data from.
            participants (int, str, list):
                Used with datasets that contain nested csvs.
                provide None to return the dataset metadata.
                provde 'all' to return a generator for each participant.
                provde a list of participant ids to return files specific to the ids.
                if participants is x with a type of int, sample the first x participants files.
            types (list):
                used when participants is set. If None, retrieve all types.
            sources (list):
                used when participants is set. If None, retrieve all sources.
            split (bool):
                used when participants is set. If true, return split dataset by type and sources
            nested_files_path (str):
                Path to participants data
            nested_files_pipeline (ProcessingOperator):
                operators to process each csv file

        Warns:
            if participant file not found during generator iteration.

        """
        # Abort if table_name isn't valid
        super().__init__()
        self._validate_table_name(table_name)

        self.participants = participants
        self.path_name = path_name
        self.table_name = table_name
        self.types = types
        self.sources = sources
        self.split = split
        self.nested_files_path = nested_files_path
        self.nested_files_pipeline = nested_files_pipeline

        if not self.nested_files_path:
            self.nested_files_path = self.path_name + "HealthKit Data/data.csv/"

    def process(self, *data_frames):
        if self.table_name == "activitysleepsurvey":
            path = pathlib.Path(self.path_name, "Activity and Sleep Survey.csv")
        elif self.table_name == "cardiodietsurvey":
            path = pathlib.Path(self.path_name, "Cardio Diet Survey.csv")
        elif self.table_name == "dailychecksurvey":
            path = pathlib.Path(self.path_name, "Daily Check Survey.csv")
        elif self.table_name == "dayonesurvey":
            path = pathlib.Path(self.path_name, "Day One Survey.csv")
        elif self.table_name == "demographics":
            path = pathlib.Path(self.path_name, "Demographics Survey.csv")
        elif self.table_name == "healthkitdata":
            path = pathlib.Path(self.path_name, "HealthKit Data.csv")
            dataframe = pd.read_csv(path)
            files_column = "data.csv"
            dataframe[files_column] = dataframe[files_column].astype(str)
            dataframe["file_name"] = dataframe[files_column] + ".gz"
            return self._process_nested_files(dataframe=dataframe, field="file_name")
        elif self.table_name == "healthkitsleep":
            path = pathlib.Path(self.path_name, "HealthKit Sleep.csv")
            dataframe = pd.read_csv(path)
            files_column = "data.csv"
            dataframe[files_column] = dataframe[files_column].astype(str)
            dataframe["file_name"] = dataframe[files_column] + ".gz"
            return self._process_nested_files(dataframe=dataframe, field="file_name")
        elif self.table_name == "heartagesurvey":
            path = pathlib.Path(self.path_name, "APH Heart Age Survey.csv")
        elif self.table_name == "parqsurvey":
            path = pathlib.Path(self.path_name, "PAR-Q Survey.csv")
        elif self.table_name == "qualityoflife":
            path = pathlib.Path(self.path_name, "Satisfied Survey.csv")
        elif self.table_name == "riskfactorsurvey":
            path = pathlib.Path(self.path_name, "Risk Factor Survey.csv")
        elif self.table_name == "sixminutewalkactivity":
            path = pathlib.Path(self.path_name, "Six Minute Walk Activity.csv")
            dataframe = pd.read_csv(path)
            files_column = "pedometer_fitness.walk.items"
            dataframe[files_column] = dataframe[files_column].astype(str)
            dataframe[files_column] = dataframe[files_column].str[
                :-2
            ]  # Remove .0 from string
            dataframe["file_name"] = dataframe[files_column] + ".gz"
            return self._process_nested_files(
                dataframe=dataframe, field="file_name", files_type="json"
            )

        return [pd.read_csv(path)]

    def _validate_table_name(self, table_name):
        if table_name not in self.valid_table_names:
            raise RuntimeError(
                f"Invalid table_name, must be from the following: {self.valid_table_names}"
            )

    def _process_nested_files(self, dataframe, field, files_type="csv"):
        nested_files_folder_path = pathlib.Path(self.nested_files_path)

        if self.types or self.sources:
            filter_op = FilterOperator(
                epoch_filter=lambda df, func=self._filter_data_func: func(df)
            )
            self.nested_files_pipeline.processing_operators.append(filter_op)

        if files_type == "csv":
            operator = ReadNestedCsvOperator(
                folder_path=nested_files_folder_path,
                field=field,
                pipeline=self.nested_files_pipeline,
            )
        else:
            operator = ReadNestedJsonOperator(
                folder_path=nested_files_folder_path,
                field=field,
                pipeline=self.nested_files_pipeline,
            )

        if not self.participants:
            return [dataframe]

        if self.participants == "all":
            output = operator.process(dataframe)
            return output

        if isinstance(self.participants, list):
            dataframe = dataframe[dataframe["recordId"].isin(self.participants)]
            generator = operator.process(dataframe)[0]
            output = list(generator)
            output = [data for data in output if self._file_exists(data[0], data[1])]

            if output:
                output = [self._join_columns(data[0], data[1]) for data in output]
                output = pd.concat(output)

        elif isinstance(self.participants, int):
            dataframe = dataframe.iloc[: self.participants]
            generator = operator.process(dataframe)[0]
            output = list(generator)
            output = [data for data in output if self._file_exists(data[0], data[1])]

            if output:
                output = [self._join_columns(data[0], data[1]) for data in output]
                output = pd.concat(output)

        if self.split and (output is not None):
            output = self._split_groups(output)
        elif output is not None:
            output = [output]

        return output

    def _filter_data_func(self, dataframe):
        if self.types and self.sources:
            return dataframe["type"].isin(self.types) & dataframe["source"].isin(
                self.sources
            )

        if self.types:
            return dataframe["type"].isin(self.types)

        if self.sources:
            return dataframe["source"].isin(self.sources)

        return None

    def _split_groups(self, dataframe):
        if self.types and self.sources:
            output = dataframe.groupby(["type", "source"])

        elif self.types:
            output = dataframe.groupby(["type"])

        elif self.sources:
            output = dataframe.groupby(["source"])

        output = list(output)
        output = [group for _, group in output]
        return output

    @staticmethod
    def _file_exists(row, data):
        if data is None:
            warnings.warn("file not found for participant:" + str(row.recordId))
            return False
        return True

    @staticmethod
    def _join_columns(row, data):
        data["recordId"] = row.recordId
        return data
