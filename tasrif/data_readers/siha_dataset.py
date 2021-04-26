"""Module that provides classes to work with a fitbit dataset dumped from SIHA
   Available datasets:
        Weight
        Physical Activity (distance, calories, heart-rate, active and sedentary minutes)
        Sleep
"""

import pathlib
import json
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.pandas import (
    JsonNormalizeOperator,
    SetIndexOperator,
    ConvertToDatetimeOperator,
    AsTypeOperator,
    DropFeaturesOperator,
)
from tasrif.processing_pipeline.custom import JqOperator, CreateFeatureOperator


class SihaDataset:
    """Base class to work with the all SIHA based datasets."""
    def __init__(self, folder, processing_pipeline):
        self.folder = folder
        fullpath = pathlib.Path(folder)
        self.processing_pipeline = processing_pipeline

        file_paths = []
        for file_path in pathlib.Path(str(fullpath)).glob("data*.json"):
            file_paths.append(str(file_path))

        if not file_paths:
            raise ValueError("No data file matching the pattern data*.json")

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


class SihaSleepDataset(SihaDataset):
    """Class to work with the sleep data (aggregated on a day basis) from a SIHA dump."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select Sleep data from the dump
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator("map({patientID} + .data.sleep[].data)"),
            JsonNormalizeOperator(
                record_path=["sleep"],
                meta=[
                    "patientID",
                    ["summary", "stages", "rem"],
                    ["summary", "stages", "deep"],
                    ["summary", "stages", "light"],
                    ["summary", "stages", "wake"],
                    ["summary", "totalMinutesAsleep"],
                    ["summary", "totalTimeInBed"],
                ],
                errors="ignore",
            ),
            ConvertToDatetimeOperator(feature_names=["dateOfSleep"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateOfSleep"),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)

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


class SihaSleepIntradayDataset(SihaDataset):
    """Class to work with intraday sleep data from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the intraday calories
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                "map({patientID} + (.data.sleep[].data as $data | "
                +
                "($data.sleep | map(.) | .[]) | . * {levels:  {overview : ($data.summary//{})}})) |  "
                +
                "map (if .levels.data != null then . else .levels += {data: []} end) | "
                +
                "map(. + {type, dateOfSleep, minutesAsleep, logId, startTime, endTime, duration, isMainSleep,"
                +
                " minutesToFallAsleep, minutesAwake, minutesAfterWakeup, timeInBed, efficiency, infoCode})"
            ),
            JsonNormalizeOperator(
                record_path=["levels", "data"],
                meta=[
                    "patientID",
                    "logId",
                    "dateOfSleep",
                    "startTime",
                    "endTime",
                    "duration",
                    "isMainSleep",
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
                    ["levels", "overview", "totalTimeInBed"],
                    ["levels", "overview", "totalMinutesAsleep"],
                    ["levels", "overview", "stages", "rem"],
                    ["levels", "overview", "stages", "deep"],
                    ["levels", "overview", "stages", "light"],
                    ["levels", "overview", "stages", "wake"],
                ],
                errors="ignore",
            ),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaCaloriesIntradayDataset(SihaDataset):
    """Class to work with the Physical Activity/calories from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the intraday calories
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                "map({patientID} + .data.activities_calories_intraday[].data as $item  |"
                +
                ' $item."activities-calories-intraday".dataset | '
                +
                'map({date: $item."activities-calories"[0].dateTime} + .) | .[])'
            ),
            JsonNormalizeOperator(),
            CreateFeatureOperator(
                feature_name="dateTime",
                feature_creator=lambda df: df["date"] + "T" + df["time"],
            ),
            DropFeaturesOperator(["date", "time"]),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "float32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaDistanceIntradayDataset(SihaDataset):
    """Class to work with the Physical Activity/distance from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the intraday distance
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                "map({patientID} + .data.activities_distance_intraday[].data as $item  |"
                +
                ' $item."activities-distance-intraday".dataset | '
                +
                'map({date: $item."activities-distance"[0].dateTime} + .) | .[])'
            ),
            JsonNormalizeOperator(),
            CreateFeatureOperator(
                feature_name="dateTime",
                feature_creator=lambda df: df["date"] + "T" + df["time"],
            ),
            DropFeaturesOperator(["date", "time"]),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "float32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaHeartRateIntradayDataset(SihaDataset):
    """Class to work with the Physical Activity/heart rate from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the intraday heartrate
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                'map({patientID} + .data.activities_heart_intraday[].data as $item  | '
                +
                '$item."activities-heart-intraday".dataset | '
                +
                'map({date: $item."activities-heart"[0].dateTime} + .) | .[])'
            ),
            JsonNormalizeOperator(),
            CreateFeatureOperator(
                feature_name="dateTime",
                feature_creator=lambda df: df["date"] + "T" + df["time"],
            ),
            DropFeaturesOperator(["date", "time"]),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "float32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaVeryActiveMinutesDataset(SihaDataset):
    """Class to work with the Physical Activity/very active minutes from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the very active minutes
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                'map({patientID} + .data.activities_tracker_minutesVeryActive[].data.'
                +
                '"activities-tracker-minutesVeryActive"[0])'
            ),
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "float32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaLightlyActiveMinutesDataset(SihaDataset):
    """Class to work with the Physical Activity/lightly active minutes from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the lightly active minutes
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                'map({patientID} + .data.activities_tracker_minutesLightlyActive[].data.'
                +
                '"activities-tracker-minutesLightlyActive"[0])'
            ),
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "float32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaSedentaryMinutesDataset(SihaDataset):
    """Class to work with the Physical Activity/sedentary minutes from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the sedentary minutes
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                'map({patientID} + .data.activities_tracker_minutesSedentary[].data.'
                +
                '"activities-tracker-minutesSedentary"[0])'
            ),
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "float32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaModeratelyActiveMinutesDataset(SihaDataset):
    """Class to work with the Physical Activity/moderately active from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the moderately active (aka fairly active) minutes
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                'map({patientID} + .data.activities_tracker_minutesFairlyActive[].data.'
                +
                '"activities-tracker-minutesFairlyActive"[0])'
            ),
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "float32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaStepsDataset(SihaDataset):
    """Class to work with the Physical Activity/steps (aggregated daily counts) from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the tracker steps
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                'map({patientID} + .data.activities_tracker_steps[].data."activities-tracker-steps"[0])'
            ),
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "int32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaCaloriesDataset(SihaDataset):
    """Class to work with the Physical Activity/caloreis (aggregated daily count) from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the tracker calories
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                'map({patientID} + .data.activities_tracker_calories[].data."activities-tracker-calories"[0])'
            ),
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "float32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaDistanceDataset(SihaDataset):
    """Class to work with the Physical Activity/distance (aggregated daily count) from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the distance
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                'map({patientID} + .data.activities_tracker_distance[].data."activities-tracker-distance"[0])'
            ),
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "float32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaCgmDataset(SihaDataset):
    """Class to work with the glucose levels measured by a CGM from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the CGM data
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator("map({patientID} + .data.cgm[])"),
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["time"],
                                      infer_datetime_format=True),
            SetIndexOperator("time"),
            AsTypeOperator({"value": "float32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaEmrDataset(SihaDataset):
    """Class to work with the EMR (electronic medical records data) from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the EMR data
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator("map({patientID} + .data.emr[])"),
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["time"],
                                      infer_datetime_format=True),
            SetIndexOperator("time"),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaTimeInHeartRateZonesDataset(SihaDataset):
    """Class to work with the time in different HR zones (aggregated daily counts) from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select HRzones data
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                'map({patientID} + .data.activities_heart[].data."activities-heart"[] as $item |'
                +
                "{dateTime: $item.dateTime, restingHeartRate: $item.value.restingHeartRate} +"
                +
                "reduce $item.value.heartRateZones[] as $i ({}; .[$i.name] = $i.minutes))"
            ),
            JsonNormalizeOperator(),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaStepsIntradayDataset(SihaDataset):
    """Class to work with the Physical Activity/steps (15 min samples) from a SIHA dump"""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select intraday steps
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                'map({patientID} + .data.activities_steps_intraday[].data as $item  | '
                +
                '$item."activities-steps-intraday".dataset | '
                +
                'map({date: $item."activities-steps"[0].dateTime} + .) | .[])'
            ),
            JsonNormalizeOperator(),
            CreateFeatureOperator(
                feature_name="dateTime",
                feature_creator=lambda df: df["date"] + "T" + df["time"],
            ),
            DropFeaturesOperator(["date", "time"]),
            ConvertToDatetimeOperator(feature_names=["dateTime"],
                                      infer_datetime_format=True),
            SetIndexOperator("dateTime"),
            AsTypeOperator({"value": "float32"}),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)
