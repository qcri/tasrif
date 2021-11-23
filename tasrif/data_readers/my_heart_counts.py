"""
Module that provides class to work with the MyHeartCounts dataset.
"""
import pathlib
import pandas as pd
from tasrif.processing_pipeline import ProcessingOperator, SequenceOperator
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator
from tasrif.processing_pipeline.custom import ReadNestedCsvOperator
from tasrif.processing_pipeline.custom import FilterOperator



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

    def __init__(self,
        path_name,
        table_name,
        participants=None,
        types=None,
        sources=None,
        split=False):
        """Initializes a dataset reader with the input parameters.

        Args:
            path_name (str):
                Path to the withings export file containing data.
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
            return self._process_healthkitdata()
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

    def _process_healthkitdata(self):
        path = pathlib.Path(self.path_name, 'HealthKit Data.csv')
        dataframe = pd.read_csv(path)
        csv_pipeline = [ConvertToDatetimeOperator(['startTime', 'endTime'], utc=True)]
        csv_folder_path = pathlib.Path(self.path_name + 'HealthKit Data/data.csv/')

        if self.types or self.sources:
            filter_op = FilterOperator(epoch_filter=lambda df, func=self._filter_data_func: func(df))
            csv_pipeline.append(filter_op)

        csv_pipeline = SequenceOperator(csv_pipeline)
        operator = ReadNestedCsvOperator(folder_path=csv_folder_path,
                                         field='_6',
                                         pipeline=csv_pipeline)

        if not self.participants:
            return [dataframe]

        if self.participants == 'all':
            output = operator.process(dataframe)
            return output

        elif isinstance(self.participants, list):
            dataframe = dataframe[dataframe['recordId'].isin(self.participants)]
            generator = operator.process(dataframe)[0]
            output = list(generator)
            output = [self._join_columns(data[0], data[1]) for data in output]
            output = pd.concat(output)

        elif isinstance(self.participants, int):
            dataframe = dataframe.iloc[:self.participants]
            generator = operator.process(dataframe)[0]
            output = list(generator)
            output = [self._join_columns(data[0], data[1]) for data in output]
            output = pd.concat(output)

        if self.split:
            output = self._split_groups(output)
        else:
            output = [output]

        return output

    def _filter_data_func(self, dataframe):
        if self.types and self.sources:
            return dataframe['type'].isin(self.types) & dataframe['source'].isin(self.sources)

        if self.types:
            return dataframe['type'].isin(self.types)

        if self.sources:
            return dataframe['source'].isin(self.sources)

    def _split_groups(self, dataframe):
        if self.types and self.sources:
            output = dataframe.groupby(['type', 'source'])

        elif self.types:
            output = dataframe.groupby(['type'])

        elif self.sources:
            output = dataframe.groupby(['source'])

        output = list(output)
        output = [group for _, group in output]
        return output


    @staticmethod
    def _join_columns(row, data):
        data['recordId'] = row.recordId
        return data
