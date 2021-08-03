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
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.pandas import (
    JsonNormalizeOperator,
    SetIndexOperator,
    ConvertToDatetimeOperator,
    AsTypeOperator,
    MergeOperator,
)
from tasrif.processing_pipeline.custom import (
    ResampleOperator,
    SetFeaturesValueOperator,
    DistributedUpsampleOperator,
    DropIndexDuplicatesOperator,
)


class FitbitIntradayDataset:
    """Base class for all fitbit intraday datasets"""
    def processed_dataframe(self):  # pylint: disable=no-self-use
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns:
            None
        """
        return None

class FitbitSleepDataset(FitbitIntradayDataset):
    """Class to work with the Sleep json files from a fitbit export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - json_normalize
        - set index to date time field
        - resample (upsample) to 5 min intervals
        """

        PIPELINE = ProcessingPipeline([
            JsonNormalizeOperator(
                record_path=["levels", "data"],
                meta=[
                    "logId",
                    "dateOfSleep",
                    "startTime",
                    "endTime",
                    "duration",
                    "minutesToFallAsleep",
                    "minutesAsleep",
                    "minutesAwake",
                    "minutesAfterWakeup",
                    "timeInBed",
                    "efficiency",
                    "type",
                    "infoCode",
                    ["levels", "summary", "deep", "count"],
                    ["levels", "summary", "deep", "minutes"],
                    ["levels", "summary", "deep", "thirtyDayAvgMinutes"],
                    ["levels", "summary", "wake", "count"],
                    ["levels", "summary", "wake", "minutes"],
                    ["levels", "summary", "wake", "thirtyDayAvgMinutes"],
                    ["levels", "summary", "light", "count"],
                    ["levels", "summary", "light", "minutes"],
                    ["levels", "summary", "light", "thirtyDayAvgMinutes"],
                    ["levels", "summary", "rem", "count"],
                    ["levels", "summary", "rem", "minutes"],
                    ["levels", "summary", "rem", "thirtyDayAvgMinutes"],
                ],
                errors="ignore",
            ),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            DropIndexDuplicatesOperator(keep="first"),
            ResampleOperator("30s", "ffill"),
            SetFeaturesValueOperator(features=["seconds"], value=30),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):
        self.folder = folder
        fullpath = pathlib.Path(folder, "Sleep")
        self.processing_pipeline = processing_pipeline

        file_paths = []
        for file_path in pathlib.Path(str(fullpath)).glob("sleep*.json"):
            file_paths.append(str(file_path))

        self.raw_df = []
        for file_path in file_paths:
            with open(file_path) as json_file:
                json_data = json.load(json_file)

                self.raw_df.extend(json_data)
        self._process()

    def _process(self):

        self.processed_df = (self.processing_pipeline.process(self.raw_df))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns:
            pd.Dataframe
                Pandas dataframe object representing the data

        """

        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns:
            pd.Dataframe
                Pandas dataframe object representing the data

        """
        return self.processed_df


class FitbitPhysicalActivityDataset(FitbitIntradayDataset):
    """Base class to work with the physical activity json files from a fitbit export dump."""
    def __init__(self, folder, file_pattern, processing_pipeline):
        self.folder = folder
        fullpath = pathlib.Path(folder, "Physical Activity")
        self.processing_pipeline = processing_pipeline

        file_paths = []
        for file_path in pathlib.Path(str(fullpath)).glob(file_pattern):
            file_paths.append(str(file_path))

        self.raw_df = []
        for file_path in file_paths:
            with open(file_path) as json_file:
                json_data = json.load(json_file)

                self.raw_df.extend(json_data)
        self._process()

    def _process(self):

        self.processed_df = (self.processing_pipeline.process(self.raw_df))[0]

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns:
            pd.Dataframe
                Pandas dataframe object representing the data

        """
        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns:
            pd.Dataframe
                Pandas dataframe object representing the data

        """
        return self.processed_df


class FitbitPhysicalActivityCaloriesDataset(FitbitPhysicalActivityDataset):
    """Class to work with the Physical Activity/calories json files from a fitbit export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - json_normalize
        - set index to date time field
        - resample (upsample) to 5 min intervals
        """

        PIPELINE = ProcessingPipeline([
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "float32"}),
            DistributedUpsampleOperator("30s"),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, "calories*.json", processing_pipeline)


class FitbitPhysicalActivityDistanceDataset(FitbitPhysicalActivityDataset):
    """Class to work with the Physical Activity/distance json files from a fitbit export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - json_normalize
        - set index to date time field
        - resample (upsample) to 5 min intervals
        """

        PIPELINE = ProcessingPipeline([
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "int32"}),
            DistributedUpsampleOperator("30s"),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, "distance*.json", processing_pipeline)


class FitbitPhysicalActivityHeartRateDataset(FitbitPhysicalActivityDataset):
    """Class to work with the Physical Activity/heart rate json files from a fitbit export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - json_normalize
        - set index to date time field
        - resample (upsample) to 30s intervals
        """

        PIPELINE = ProcessingPipeline([
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({
                "value.bpm": "int32",
                "value.confidence": "int32"
            }),
            DropIndexDuplicatesOperator(keep="first"),
            ResampleOperator("30s", {
                "value.bpm": "mean",
                "value.confidence": "mean"
            }),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, "heart_rate*.json", processing_pipeline)


class FitbitPhysicalActivityVeryActiveMinutesDataset(
        FitbitPhysicalActivityDataset):
    """Class to work with the Physical Activity/very active minutes json files from a fitbit export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "int32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, "very_active_minutes*.json",
                         processing_pipeline)


class FitbitPhysicalActivityLightlyActiveMinutesDataset(
        FitbitPhysicalActivityDataset):
    """Class to work with the Physical Activity/lightly active minutes json files from a fitbit export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - json_normalize
        - set index to date time field
        - resample (upsample) to 5 min intervals
        """

        PIPELINE = ProcessingPipeline([
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "int32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, "lightly_active_minutes*.json",
                         processing_pipeline)


class FitbitPhysicalActivitySedentaryMinutesDataset(
        FitbitPhysicalActivityDataset):
    """Class to work with the Physical Activity/sedentary minutes json files from a fitbit export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "int32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, "sedentary_minutes*.json",
                         processing_pipeline)


class FitbitPhysicalActivityModeratelyActiveMinutesDataset(
        FitbitPhysicalActivityDataset):
    """Class to work with the Physical Activity/moderately active json files from a fitbit export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "int32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, "moderately_active_minutes*.json",
                         processing_pipeline)


class FitbitPhysicalActivityTimeInHeartRateZonesDataset(
        FitbitPhysicalActivityDataset):
    """Class to work with the Physical Activity/time in HR zones json files from a fitbit export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({
                "value.valuesInZones.IN_DEFAULT_ZONE_3":
                "float32",
                "value.valuesInZones.IN_DEFAULT_ZONE_1":
                "float32",
                "value.valuesInZones.IN_DEFAULT_ZONE_2":
                "float32",
                "value.valuesInZones.BELOW_DEFAULT_ZONE_1":
                "float32",
            }),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, "time_in_heart_rate_zones*.json",
                         processing_pipeline)


class FitbitPhysicalActivityStepsDataset(FitbitPhysicalActivityDataset):
    """Class to work with the Physical Activity/steps json files from a fitbit export dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high levelsteps:
        - json_normalize
        - set index to date time field
        - resample (upsample) to 5 min intervals
        """

        PIPELINE = ProcessingPipeline([
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "int32"}),
            DistributedUpsampleOperator("30s"),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, "steps*.json", processing_pipeline)


class FitbitIntradayCompositeDataset:
    """
    Class to work with exported fitbit intraday dataset
    """
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class"""

        PIPELINE = ProcessingPipeline(
            [MergeOperator(on="dateTime", how="outer")])

    def __init__(self, datasets, processing_pipeline=Default.PIPELINE):  # pylint: disable=too-few-public-methods
        self.datasets = datasets
        self.processing_pipeline = processing_pipeline
        self._process()

    def _process(self):
        datasets = []
        for dataset in self.datasets:
            datasets.append(dataset)

        self.processed_df = self.processing_pipeline.process(*datasets)

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns:
            pd.Dataframe
                Pandas dataframe object representing the data

        """
        return self.processed_df
