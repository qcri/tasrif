"""Module that provides classes to access sleep health datasets.
For details please read https://www.nature.com/articles/s41597-020-00753-2
"""
import pathlib
import numpy as np
import pandas as pd

from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.pandas import DropNAOperator, DropDuplicatesOperator, ReplaceOperator
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SortOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator, AggregateOperator, OneHotEncoderOperator


class SleepHealthDataset:  # pylint: disable=too-few-public-methods
    """
    Class to work with Sleep Health Dataset
    """

    def __init__(self, shc_folder='../data/sleephealth/'):
        self.shc_folder = shc_folder
        # TODO: placeholder only, missing implementation


class AboutMeDataset:
    """Provides access to the AboutMe dataset

    Returns
    -------
    Instance of AboutMeDataset
    """
    processed_df = None
    raw_df = None

    def __init__(self,
                 shc_folder: str = '../data/sleephealth/',
                 dataset_filename: str = 'About Me.csv',
                 pipeline: ProcessingPipeline = ProcessingPipeline(
                     [DropNAOperator(subset=["alcohol", "basic_expenses", "caffeine", "daily_activities",
                                             "daily_smoking", "education", "flexible_work_hours", "gender",
                                             "good_life", "hispanic", "income", "race", "work_schedule", "weight",
                                             "smoking_status", "marital"]),
                      DropDuplicatesOperator(subset='participantId',
                                             keep='last')
                      ]
                 )
                 ):
        """
        AboutMe Dataset details can be found online at ``https://www.synapse.org/#!Synapse:syn18492837/wiki/592581``.

        Some important stats:
            - This dataset contains unique data for 3448 participants.
            - ``alcohol`` has 10 NAs (10/3448 = 0.29%)
            - ``basic_expenses`` has 20 NAs (20/3448 = 0.58%)
            - ``caffeine`` has 30 NAs (30/3448 = 0.87%)
            - ``daily_activities`` has 6 NAs (6/3448 = 0.17%)
            - ``daily_smoking`` has 11 NAs (11/3448 = 0.32%)
            - ``education`` has 17 NAs (17/3448 = 0.49%)
            - ``flexible_work_hours`` has 105 NAs (105/3448 = 3.05%)
            - ``gender`` has 8 NAs (8/3448 = 0.23%)
            - ``good_life`` has 10 NAs (10/3448 = 0.29%)
            - ``hispanic`` has 7 NAs (7/3448 = 0.20%)
            - ``income`` has 28 NAs (28/3448 = 0.81%)
            - ``marital`` has 7 NAs (7/3448 = 0.20%)
            - ``race`` has 8 NAs (8/3448 = 0.23%)
            - ``smoking_status`` has 21 NAs (21/3448 = 0.61%)
            - ``weight`` has 59 NAs (59/3448 = 1.71%)
            - ``menopause`` has 2340 NAs (2340/3448 = 67.87%)
            - ``recent_births`` has 2309 NAs (2309/3448 = 66.97%)
            - ``current_pregnant`` has 3438 NAs (3438/3448 = 99.71%)
            - ``work_schedule`` has 110 NAs (110/3448 = 3.19%)

        The default behavior of this module is to
         (1) remove NAs for participants in all columns, but ``menopause``, ``recent_births`` and ``current_pregnant``.
         (2) Drop duplicates based on participant id, retaining the last occurrence of a participant id.
         The default final dataset size is 3019.

        """

        full_path = pathlib.Path(shc_folder, dataset_filename)
        self.raw_df = pd.read_csv(full_path)
        self.processed_df = self.raw_df.copy()
        self.pipeline = pipeline
        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        return self.raw_df['participantId'].nunique()

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

    def _process(self):
        if self.pipeline:
            self.processed_df = self.pipeline.process(self.processed_df)[0]


class SleepQualityCheckerDataset:
    """Provides access to the SleepQualityChecker dataset

    Returns
    -------
    Instance of SleepQualityCheckerDataset
    """

    def __init__(
        self,
        shc_folder: str = '../data/sleephealth/',
        dataset_filename: str = 'Sleep Quality Checker.csv',
        pipeline: ProcessingPipeline = ProcessingPipeline([
            SortOperator(by=["participantId", "timestamp"]),
            AggregateOperator(groupby_feature_names="participantId",
                              aggregation_definition={
                                  "sq_score": ["count", "mean", "std", "min", "max", "first", "last"],
                                  "timestamp": ["first", "last"]
                              }),
            ConvertToDatetimeOperator(feature_names=["timestamp_last", "timestamp_first"], format="%Y-%m-%dT%H:%M:%S%z",
                                      utc=True),
            CreateFeatureOperator(feature_name="delta_first_last_timestamp",
                                  feature_creator=lambda row: row['timestamp_last'] - row['timestamp_first'])
        ])
    ):
        """
        Sleep Quality Checker Dataset details can be found online at ``https://www.synapse.org/#!Synapse:syn18492837/wiki/593719``.

        Some important stats:
            - This dataset contains unique data for 4,566 participants.
            - The default pipeline groups multiple entries for different `participantId` into one row per participant
              and multiple column with statistics for the sleep quality score (`sq_score`) of each participant.

        """

        full_path = pathlib.Path(shc_folder, dataset_filename)
        self.raw_df = pd.read_csv(full_path)

        self.processed_df = self.raw_df.copy()
        self.pipeline = pipeline
        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        return self.raw_df['participantId'].nunique()

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

    def _process(self):
        if self.pipeline:
            self.processed_df = self.pipeline.process(self.processed_df)[0]


class OnboardingDemographicsDataset:
    """Provides access to the OnboardingDemographics dataset

    Returns
    -------
    Instance of OnboardingDemographicsDataset
    """

    def __init__(
        self,
        shc_folder: str = '../data/sleephealth/',
        dataset_filename: str = 'Onboarding Demographics.csv',
        pipeline: ProcessingPipeline = ProcessingPipeline([ReplaceOperator(to_replace="CENSORED", value=np.nan),
                                                           DropNAOperator()])
    ):
        """
        Sleep Quality Checker Dataset details can be found online at ``https://www.synapse.org/#!Synapse:syn18492837/wiki/590798``.
        According to the documentation, Height values <60in or >78in and weight values <80 or >350 have been censored to protect participants with potentially identifying features.
        The default pipeline works like that:
            1. replaces the "CENSORED" values presented in this dataset by NAs.
            2. Drops NAs:
                2.(a) gender: 89 (1.09%)
                2.(b) age_years: 54 (0.66%)
                2.(c) weight_pounds: 431 (5.3%)
                2.(d) height_inches: 337 (4.1%)
        The final dataset size after removing all NAs is 7558 (retaining 93% of the original dataset).
        """
        full_path = pathlib.Path(shc_folder, dataset_filename)
        self.raw_df = pd.read_csv(full_path)
        self.processed_df = self.raw_df.copy()
        self.pipeline = pipeline
        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        return self.raw_df['participantId'].nunique()

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

    def _process(self):
        if self.pipeline:
            self.processed_df = self.pipeline.process(self.processed_df)[0]


class SleepHabitDataset:
    """Provides access to the Sleep Habit dataset

    Returns
    -------
    Instance of SleepHabitDataset
    """

    def __init__(
        self,
        shc_folder: str = '../data/sleephealth/',
        dataset_filename: str = 'Sleep Habits.csv',
        pipeline: ProcessingPipeline = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names=["timestamp"], format="%Y-%m-%dT%H:%M:%S", utc=True),
            ReplaceOperator(to_replace={"driving_sleepy": {6: np.nan},
                                        "morning_person": {3: np.nan},
                                        "nap_duration": {6: np.nan},
                                        "what_wakes_you": {13: np.nan}}),
            DropNAOperator(subset=["alarm_dependency", "driving_sleepy", "falling_asleep", "sleep_needed",
                                   "sleep_partner", "sleep_time_workday", "wake_up_choices", "wake_ups",
                                   "what_wakes_you"]),
            OneHotEncoderOperator(feature_names=["alarm_dependency", "driving_sleepy", "falling_asleep",
                                                 "morning_person", "nap_duration", "sleep_partner",
                                                 "wake_up_choices", "weekly_naps", "what_wakes_you"])
        ])
    ):
        """
        Sleep Habit Dataset details can be found online at ``https://www.synapse.org/#!Synapse:syn18492837/wiki/593720``.

        Full Shape: (3303, 16)
        Some important stats:
            - This dataset contains 3303  rows.
             - `` participantId `` has 0 NAs ( 3303 / 3303 ) = 0.00 %
             - `` alarm_dependency `` has 17 NAs ( 3286 / 3303 ) = 0.51 %
             - `` driving_sleepy `` has 27 NAs ( 3276 / 3303 ) = 0.82 %
             - `` falling_asleep `` has 9 NAs ( 3294 / 3303 ) = 0.27 %
             - `` morning_person `` has 9 NAs ( 3294 / 3303 ) = 0.27 %
             - `` nap_duration `` has 1539 NAs ( 1764 / 3303 ) = 46.59 %
             - `` sleep_lost `` has 1495 NAs ( 1808 / 3303 ) = 45.26 %
             - `` sleep_needed `` has 49 NAs ( 3254 / 3303 ) = 1.48 %
             - `` sleep_partner `` has 8 NAs ( 3295 / 3303 ) = 0.24 %
             - `` sleep_time_workday `` has 15 NAs ( 3288 / 3303 ) = 0.45 %
             - `` sleep_time_weekend `` has 14 NAs ( 3289 / 3303 ) = 0.42 %
             - `` wake_up_choices `` has 7 NAs ( 3296 / 3303 ) = 0.21 %
             - `` wake_ups `` has 29 NAs ( 3274 / 3303 ) = 0.88 %
             - `` weekly_naps `` has 6 NAs ( 3297 / 3303 ) = 0.18 %
             - `` what_wakes_you `` has 13 NAs ( 3290 / 3303 ) = 0.39 %
             - `` timestamp `` has 0 NAs ( 3303 / 3303 ) = 0.00 %

        The default pipeline:
            (1) converts timestamp col to datatime;
            (2) transforms into NA some values (i.e. answer for a question being "Don't known") in the following
            columns: "driving_sleepy", "morning_person", "nap_duration", "what_wakes_you".
            (3) drop rows in which the number of NA is smaller than 5% of the rows.
                Accumulatively, this step removes 192 rows (5.8% of the original number of rows).
            (4) One hot encode categorical features.

        """
        full_path = pathlib.Path(shc_folder, dataset_filename)
        self.raw_df = pd.read_csv(full_path)
        self.processed_df = self.raw_df.copy()
        self.pipeline = pipeline
        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        return self.raw_df['participantId'].nunique()

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

    def _process(self):
        if self.pipeline:
            self.processed_df = self.pipeline.process(self.processed_df)[0]
