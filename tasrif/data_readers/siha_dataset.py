"""Module that provides classes to work with a fitbit dataset dumped from SIHA
   Available datasets:
        Weight
        Physical Activity (distance, calories, heart-rate, active and sedentary minutes)
        Sleep
"""

import pathlib
import json

import numpy as np
import pandas as pd
import seaborn as sns

from tasrif.processing_pipeline import (
    ProcessingPipeline,
    ComposeOperator,
    NoopOperator,
    SequenceOperator,
)

from tasrif.processing_pipeline.pandas import (
    JsonNormalizeOperator,
    SetIndexOperator,
    ConvertToDatetimeOperator,
    AsTypeOperator,
    DropFeaturesOperator,
    RenameOperator,
    ResetIndexOperator,
    DropNAOperator,
    GroupbyOperator,
    SumOperator,
    CorrOperator,
    ApplyOperator,
    PivotOperator,
    MergeOperator,
)

from tasrif.processing_pipeline.custom import (JqOperator,
                                               CreateFeatureOperator,
                                               ResampleOperator,
                                               SetFeaturesValueOperator,
                                               FunctionOperator, LogOperator)


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
    """Class to work with the sleep data (aggregated on a day basis) from a SIHA dump.
    """
    class Default:  #pylint: disable=too-few-public-methods
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
    class Default:  #pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        The default pipeline consists of the following high level steps:
        - JQ query to select the intraday calories
        - json_normalize
        - set index to date time field
        """

        PIPELINE = ProcessingPipeline([
            JqOperator(
                "map({patientID} + (.data.sleep[].data as $data | " +
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
                + ' $item."activities-calories-intraday".dataset | ' +
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
            ResetIndexOperator(),
            RenameOperator(columns={
                "dateTime": "time",
                "value": "Calories"
            }),
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
                + ' $item."activities-distance-intraday".dataset | ' +
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
            ResetIndexOperator(),
            RenameOperator(columns={
                "dateTime": "time",
                "value": "Distance"
            }),
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
                + '$item."activities-heart-intraday".dataset | ' +
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
            ResetIndexOperator(),
            RenameOperator(columns={
                "dateTime": "time",
                "value": "HeartRate"
            }),
            GroupbyOperator(by="patientID", select=["HeartRate", "time"]),
            ResampleOperator(rule='15min',
                             offset='00h00min',
                             on='time',
                             aggregation_definition='mean'),
            ResetIndexOperator(),
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
                + '"activities-tracker-minutesVeryActive"[0])'),
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
                + '"activities-tracker-minutesLightlyActive"[0])'),
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
                + '"activities-tracker-minutesSedentary"[0])'),
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
                + '"activities-tracker-minutesFairlyActive"[0])'),
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
            ResetIndexOperator(),
            RenameOperator(columns={"value": "CGM"}),
            GroupbyOperator(by="patientID", select=["CGM", "time"]),
            ResampleOperator(rule='15min',
                             offset='00h00min',
                             on='time',
                             aggregation_definition='mean'),
            ResetIndexOperator()
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaEmrDataset:  #pylint: disable=too-few-public-methods
    """Class to work with the EMR (electronic medical records data) from a SIHA dump
    """
    class Default:  #pylint: disable=too-few-public-methods
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
            ResetIndexOperator(),
            PivotOperator(index="patientID",
                          columns="variable",
                          values="value"),
            CreateFeatureOperator(
                feature_name='Diabetes Duration',
                feature_creator=lambda df: np.nan
                if df['Diabetes Duration'] == '' else df['Diabetes Duration']),
            AsTypeOperator({
                'BMI': float,
                'Cholesterol': float,
                'Creatinine': float,
                'Diabetes Duration': float,
                'Diastolic Blood Pressure': float,
                'HDL': float,
                'HbA1c': float,
                'LDL': float,
                'Systolic Blood Pressure': float,
            }),
            ComposeOperator([
                NoopOperator(),
                SequenceOperator([
                    # This should get rid of the dosage
                    #(Glargine 300 or Glargine 100 -> Glargine)
                    FunctionOperator(lambda df: df['Diabetes Medication']),
                    ApplyOperator(lambda s: s.replace("+", ", ").split(",")),
                    ApplyOperator(
                        lambda l:
                        [x.split()[0].strip() for x in l if x.strip()]),
                ])
            ])
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):
        self.folder = folder
        fullpath = pathlib.Path(folder)
        self.processing_pipeline = processing_pipeline

        file_paths = []
        for file_path in pathlib.Path(str(fullpath)).glob("data*.json"):
            file_paths.append(str(file_path))

        self.raw_df = []
        for file_path in file_paths:
            with open(file_path) as json_file:
                json_data = json.load(json_file)

                self.raw_df.extend(json_data)
        self._process()

    def _process(self):
        self.processed_df = (self.processing_pipeline.process(self.raw_df))


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
                + '$item."activities-steps-intraday".dataset | ' +
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
            ResetIndexOperator(),
            RenameOperator(columns={
                "dateTime": "time",
                "value": "Steps"
            }),
        ])

    def __init__(self, folder, processing_pipeline=Default.PIPELINE):

        super().__init__(folder, processing_pipeline)


class SihaCompositeDataset:
    """Class to work with merged datasets of
        SihaHeartRateIntradayDataset,
        SihaCaloriesIntradayDataset,
        SihaStepsIntradayDataset,
        SihaDistanceIntradayDataset.
    """
    class Default:  #pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        """
        @staticmethod
        def ramadan_flag(time,
                         start_ramadan=pd.Timestamp(2020, 4, 23, 0, 0, 0),
                         end_ramadan=pd.Timestamp(2020, 5, 23, 23, 59, 59)):
            """ Ramadan flag
            """
            if (time >= start_ramadan) & (time <= end_ramadan):
                return 1
            if time < start_ramadan:
                return 0
            return 2

        @staticmethod
        def is_hypo(cgm_value):
            """ below 70 is hypoglycemia
            """
            if cgm_value <= 70:
                return 1
            return 0

        @staticmethod
        def is_hyper(cgm_value):
            """ above 180 is hyperglycemia
            """
            if cgm_value >= 180:
                return 1
            return 0

        MIN_STEPS = 1000

        @staticmethod
        def drop_days_below_min_steps(dataframe,
                                      min_steps=1000,
                                      time_col="time",
                                      pid_col="patientID"):
            """ Drops days in df that contain <= min_steps
            """
            # Get number of steps in a day
            total_steps_day = ProcessingPipeline([
                GroupbyOperator(
                    by=[pid_col, pd.Grouper(key=time_col, freq='D')],
                    select=["Steps"]),
                SumOperator()
            ]).process(dataframe)[0]

            total_steps_day = total_steps_day['Steps']

            # Find the <pids, days> to drop
            days_to_drop = total_steps_day[total_steps_day <= min_steps].index

            # Temporarily reindex dataframe with <pid, day>
            # Return only the <pids, days> that are not in the list to drop
            # The first level of index (pid) needs to come back to the dataframe,
            # but the second one (day) have to be dropped
            pipeline = ProcessingPipeline([
                SetIndexOperator([pid_col, dataframe[time_col].dt.floor("D")]),
                SetFeaturesValueOperator(
                    selector=lambda df: ~df.index.isin(days_to_drop)),
                ResetIndexOperator(level=0),
                ResetIndexOperator(drop=True),
            ])

            df_tmp = pipeline.process(dataframe)[0]

            return df_tmp

        @staticmethod
        def create_data_availability_dict(dataframe):
            """ Finds number of days before, during, and after Ramadan per patient in dataframe
            """
            tmp = {}
            tmp["before"] = dataframe[(
                dataframe["Ramadan"] == 0
            )]["time"].dt.floor("d").unique().shape[0]
            tmp["during"] = dataframe[(
                dataframe["Ramadan"] == 1
            )]["time"].dt.floor("d").unique().shape[0]
            tmp["after"] = dataframe[(
                dataframe["Ramadan"] == 2
            )]["time"].dt.floor("d").unique().shape[0]
            tmp["total"] = tmp["before"] + tmp["during"] + tmp["after"]
            tmp = pd.Series(tmp)
            return tmp

        GOOD_AVAILABILITY = SequenceOperator([
            GroupbyOperator(by="patientID"),
            ApplyOperator(lambda group, fun=create_data_availability_dict: fun.
                          __func__(group)),
            FunctionOperator(lambda df: df[(df["before"] >= 5) &
                                           (df["during"] >= 5)])
        ])

        BEFORE_RAMADAN = SequenceOperator([
            FunctionOperator(lambda df: df[df["Ramadan"] == 0]),
            ResetIndexOperator(drop=True),
            FunctionOperator(lambda df: df.drop(columns='Ramadan')),
        ])

        DURING_RAMADAN = SequenceOperator([
            FunctionOperator(lambda df: df[df["Ramadan"] == 1]),
            ResetIndexOperator(drop=True),
            AsTypeOperator({'hypo': int}),
            FunctionOperator(lambda df: df.drop(columns='Ramadan')),
        ])

        PIPELINE = ProcessingPipeline([
            MergeOperator(on=['patientID', 'time'], how='outer'),
            DropNAOperator(),
            CreateFeatureOperator(feature_name='Ramadan',
                                  feature_creator=lambda df, fun=ramadan_flag:
                                  fun.__func__(df['time'])),
            CreateFeatureOperator(
                'hypo', lambda df, fun=is_hypo: fun.__func__(df['CGM'])),
            CreateFeatureOperator(
                'hyper', lambda df, fun=is_hyper: fun.__func__(df['CGM'])),
            DropNAOperator(),
            # Dropping days below minimum
            # Next, we are going to perform a few data cleaning procedures.
            # Note that a large number of epochs (almost 43% of all FitBit datapoints) miss the corresponding CGM data.
            # As we are going to later predict CGM, we will now drop invalid CGM data instead of imputing it.
            FunctionOperator(
                lambda df, fun=drop_days_below_min_steps: fun.__func__(df)),
            ComposeOperator([
                NoopOperator(),
                GOOD_AVAILABILITY,
            ]),
            FunctionOperator(lambda df, good_availability: df[0][df[0][
                "patientID"].isin(good_availability[0].index.to_list())]),
            LogOperator(function=lambda df: df["patientID"].unique().shape[0],
                        string="Remaining number of participants:"),
            # Analyze data
            ComposeOperator([BEFORE_RAMADAN, DURING_RAMADAN,
                             NoopOperator()]),
        ])

        #

    def __init__(self,
                 profast_folder='~/Documents/Data/',
                 processing_pipeline: ProcessingPipeline = Default.PIPELINE):
        self.processed_df = None
        self.raw_dfs = None
        self.profast_folder = profast_folder
        self.processing_pipeline = processing_pipeline
        self._load_datasets()
        self._process()

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.raw_dfs

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.processed_df

    def _load_datasets(self):
        """ Function to load Siha datasets into the class, sets the result into self.raw_dfs
        """

        df_intra = SihaHeartRateIntradayDataset(
            folder=self.profast_folder).processed_dataframe()
        df_calories = SihaCaloriesIntradayDataset(
            folder=self.profast_folder).processed_dataframe()
        df_steps = SihaStepsIntradayDataset(
            folder=self.profast_folder).processed_dataframe()
        df_distance = SihaDistanceIntradayDataset(
            folder=self.profast_folder).processed_dataframe()
        df_cgm = SihaCgmDataset(
            folder=self.profast_folder).processed_dataframe()

        self.raw_dfs = (df_intra, df_calories, df_steps, df_distance, df_cgm)

    def _process(self):
        """Modifies self.cd_df by dropping columns (features) that
        are given in self.drop_features

        Returns
        -------
        sets the result in self.processed_df
        """
        self.processed_df = self.processing_pipeline.process(*self.raw_dfs)

    def boxplot_correlation(self,
                            dataframe,
                            secondary_col,
                            main_col="CGM",
                            remove_zero_steps=True):
        """ Draws correlation plot
        """

        if remove_zero_steps:
            df_tmp = dataframe[dataframe["Steps"] > 0].copy()
        else:
            df_tmp = dataframe.copy()

        # Get day correlation
        pipeline = ProcessingPipeline([
            CreateFeatureOperator(
                feature_name='Date',
                feature_creator=lambda df: df['time'].date()),
            GroupbyOperator(by=['patientID', 'Date']),
            CorrOperator(),
            ResetIndexOperator(),
            FunctionOperator(lambda df: df[df["level_2"] == main_col]),
            CreateFeatureOperator(feature_name='Ramadan',
                                  feature_creator=lambda df, fun=self.Default.ramadan_flag: fun(df['Date']))
        ])

        df_tmp = pipeline.process(df_tmp)[0]

        # Plot the orbital period with horizontal boxes
        sns.set_theme(style="ticks")
        plot = sns.boxplot(x='patientID',
                           y=secondary_col,
                           hue="Ramadan",
                           data=df_tmp)
        sns.stripplot(x='patientID',
                      y=secondary_col,
                      data=df_tmp,
                      size=4,
                      color=".3",
                      linewidth=0)

        plot.set(ylabel='Pearson Correlation\n%s-%s' %
                 (main_col, secondary_col))
        sns.despine(offset=10, trim=True)

        # Move the legend to the right side
        plot.legend(title="Ramadan?", bbox_to_anchor=(1.3, 0.5), ncol=1)
        return plot
