"""Module that provides classes to work with a exported withings dataset
   Available datasets:
        Nutrition (food and water logs)
        Weight
        Physical Activity (distance, calories, heart-rate, active and sedentary minutes)
        Sleep
"""

import pathlib
import json
import datetime
import re
import pandas as pd
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.pandas import SetIndexOperator, ConvertToDatetimeOperator, AsTypeOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator, AggregateOperator

class WithingsDataset:
    """Base class for all withings datasets
    """

    raw_df = None

    def processed_dataframe(self):  # pylint: disable=no-self-use
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return None

    def _expand_duration_to_times(self, name):

        rows = []

        df_temp = self.raw_df.copy()

        # Cast value from string to list
        df_temp['value'] = df_temp.value.apply(lambda x: x[1:-1].split(','))

        # Cast duration from string to list of ints
        df_temp['duration'] = df_temp.duration.apply(lambda x: x[1:-1].split(','))
        df_temp['duration'] = df_temp.duration.apply(lambda x: [int(i) for i in x])

        df_temp.apply(lambda row: [rows.append([datetime.datetime.fromisoformat(row['start']) +

            datetime.timedelta(seconds=sum(row['duration'][0:index])), datetime.datetime.fromisoformat(row['start']) +

            datetime.timedelta(seconds=sum(row['duration'][0:index+1])), row['value'][index]])
                              for index, end in enumerate(row['duration'])], axis=1)

        df_new = pd.DataFrame(rows, columns=['from', 'to', name])

        return df_new

class WithingsSleepDataset(WithingsDataset):
    """Class to work with the Sleep csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'from' and 'to' fields to DateTime
        - copies 'to' into a new field, Date
        - groups rows by the Date field and sums up features
        - set index to Date field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True),
            CreateFeatureOperator(
                feature_name="Date",
                feature_creator=lambda df: df['to'].date()),
            AggregateOperator(groupby_feature_names="Date",
                              aggregation_definition={
                                  'Heart rate (min)': 'mean',
                                  'Heart rate (max)': 'mean',
                                  'Average heart rate': 'mean',
                                  'Duration to sleep (s)': 'sum',
                                  'Duration to wake up (s)': 'sum',
                                  'Snoring (s)': 'sum',
                                  'Snoring episodes': 'sum',
                                  'rem (s)': 'sum',
                                  'light (s)': 'sum',
                                  'deep (s)': 'sum',
                                  'awake (s)': 'sum',
                              }),
            SetIndexOperator('Date'),
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'sleep.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):
        self.processed_df = (self.processing_pipeline.process(self.raw_df))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsStepsIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Steps csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"steps": "int32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_steps.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('steps')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsVerticalRadiusIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Vertical Radius csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"vertical_radius": "int32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_vertical-radius.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('vertical_radius')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsSleepStateIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Sleep State csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"sleep_state": "int32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_sleep-state.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('sleep_state')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsQualityScoreIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Quality Score csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"quality_score": "int32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_quality_score.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('quality_score')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsLapPoolIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Lap pool csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"lap_pool": "int32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_lap-pool.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('lap_pool')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsHeartRateIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Heart Rate csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"heart_rate": "int32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_hr.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('heart_rate')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsHorizontalRadiusIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Horizontal Radius csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"horizontal_radius": "int32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_horizontal-radius.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('horizontal_radius')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsGPSSpeedIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker GPS Speed csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"gps_speed": "float32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_gps-speed.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('gps_speed')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsElevationIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Elevation csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"elevation": "int32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_elevation.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('elevation')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsDistanceIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Distance csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"distance": "float32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_distance.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('distance')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsDurationIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Lap pool csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"duration": "int32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_duration.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('duration')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsCaloriesIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Calories Earned csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"calories": "float32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_calories-earned.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('calories')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsSPO2IntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker SPO2 csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"spo2": "int32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_auto_spo2.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('spo2')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsAltitudeIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Altitude csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"altitude": "float32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_altitude.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_new = self._expand_duration_to_times('altitude')

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsLatLongIntradayDataset(WithingsDataset):
    """Class to work with the Raw Tracker Vertical Radius csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'from' and 'to' fields to DateTime
        - copies 'to' into a new field, Date
        - groups rows by the Date field and sums up features
        - set index to Date field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
            AsTypeOperator({"latitude": "float32", "longitude": "float32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'raw_tracker_latitude.csv'), "r") as file_object:
            file_object.seek(0)
            df_latitude = pd.read_csv(file_object)

        with open(pathlib.Path(folder+'raw_tracker_longitude.csv'), "r") as file_object:
            file_object.seek(0)
            df_longitude = pd.read_csv(file_object)

        df_latitude = df_latitude.rename(columns={"value": "latitude"})
        df_longitude = df_longitude.rename(columns={"value": "longitude"})

        longitude_values = df_longitude["longitude"]
        df_latitude = df_latitude.join(longitude_values)

        self.raw_df = df_latitude

        self._process()

    def _process(self):

        rows = []

        df_temp = self.raw_df

        # Cast value from string to list
        df_temp['latitude'] = df_temp.latitude.apply(lambda x: x[1:-1].split(','))
        df_temp['longitude'] = df_temp.longitude.apply(lambda x: x[1:-1].split(','))

        # Cast duration from string to list of ints
        df_temp['duration'] = df_temp.duration.apply(lambda x: x[1:-1].split(','))
        df_temp['duration'] = df_temp.duration.apply(lambda x: [int(i) for i in x])

        df_temp.apply(lambda row: [rows.append([datetime.datetime.fromisoformat(row['start']) +
        datetime.timedelta(seconds=sum(row['duration'][0:index])), datetime.datetime.fromisoformat(row['start']) +
        datetime.timedelta(seconds=sum(row['duration'][0:index+1])), row['latitude'][index], row['longitude'][index]])
                              for index, end in enumerate(row['duration'])], axis=1)

        df_new = pd.DataFrame(
            rows, columns=['from', 'to', 'latitude', 'longitude'])

        self.processed_df = (self.processing_pipeline.process(df_new))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsWeightDataset:
    """Class to work with the Weight csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["Date"],
                                      infer_datetime_format=True),
            SetIndexOperator("Date"),
            AsTypeOperator({"Weight (kg)": "float32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'weight.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        self.processed_df = (self.processing_pipeline.process(self.raw_df))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsBPDataset:
    """Class to work with the BP csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["Date"],
                                      infer_datetime_format=True),
            SetIndexOperator("Date"),
            AsTypeOperator(
                {"Heart rate": "int32", "Systolic": "float32", "Diastolic": "float32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'bp.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        self.processed_df = (self.processing_pipeline.process(self.raw_df))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsHeightDataset:
    """Class to work with the Height csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["Date"],
                                      infer_datetime_format=True),
            SetIndexOperator("Date"),
            AsTypeOperator({"height": "float32"})
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'height.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_temp = self.raw_df

        reg_ex = re.compile(r"([0-9]+)' ([0-9]*\.?[0-9]+)")

        # Convert height unit from in to cm
        df_temp = df_temp.rename(columns={"Height (in)": "height"})
        df_temp['height'] = df_temp.height.apply(lambda x: float('NaN') if reg_ex.match(
            x) is None else 2.54*(int(reg_ex.match(x).group(1))*12 + float(reg_ex.match(x).group(2))))

        self.processed_df = (self.processing_pipeline.process(df_temp))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df

class WithingsActivitiesDataset:
    """Class to work with the Height csv files from a withings export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - converts 'start' and 'end' fields to DateTime
        - set index to start field
        """

        PIPELINE = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["from", "to"],
                                      infer_datetime_format=True, utc=True),
            SetIndexOperator("from"),
        ])

    def __init__(self, folder, processing_pipeline=None):
        """Initializes an interday dataset reader with the input parameters.

        Parameters
        ----------
        file: str
            Path to the fitbit export folder containing withings data.

        processing_pipeline: ProcessingPipeline
            The processing pipeline to be applied on the extracted data.
            If no pipeline is passed, uses Default.PIPELINE set in the class.
        """
        self.folder = folder
        self.processing_pipeline = processing_pipeline if processing_pipeline else self.Default.PIPELINE

        with open(pathlib.Path(folder+'activities.csv'), "r") as file_object:
            file_object.seek(0)
            data_frame = pd.read_csv(file_object)

        self.raw_df = data_frame

        self._process()

    def _process(self):

        df_temp = self.raw_df

        # Converting String to JSON before normalizing the JSON Data and GPS columns

        df_temp['Data'] = df_temp.Data.apply(json.loads)
        df_temp['GPS'] = df_temp['GPS'].fillna('{}')
        df_temp['GPS'] = df_temp.GPS.apply(json.loads)

        df_1 = pd.DataFrame(df_temp['Data'].values.tolist())
        df_2 = pd.DataFrame(df_temp['GPS'].values.tolist())

        df_temp = pd.concat([df_temp.drop('Data', axis=1), df_1], axis=1)
        df_temp = pd.concat([df_temp.drop('GPS', axis=1), df_2], axis=1)

        self.processed_df = (self.processing_pipeline.process(df_temp))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.processed_df
