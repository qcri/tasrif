"""Module that provides classes to access sleep health datasets.
For details please read https://www.nature.com/articles/s41597-020-00753-2

Datasets Included in this module (X for the currently implemented ones):

-- One Time Questionnaires:
(X) Onboarding Demographics
(X) About Me
(X) My Family
(X) My Health
(X) Research Interest
(X) Sleep Assessment
(X) Sleep Habits

-- Recurrent Questionnaires
( ) Nap Tracker
( ) AM Check-in
( ) PM Check-in
(X) Sleep Quality Checker
( ) Sleepiness Checker
( ) Alertness Checker - Psychomotor Vigilance Task

-- Wearable Data:
( ) HealthKit Data

"""
import pathlib
import numpy as np
import pandas as pd

from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.pandas import (
    DropNAOperator,
    DropDuplicatesOperator,
    DropFeaturesOperator,
    ReplaceOperator,
)
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SortOperator
from tasrif.processing_pipeline.custom import (
    CreateFeatureOperator,
    AggregateOperator,
    OneHotEncoderOperator,
)


class SleepHealthDataset:  # pylint: disable=too-few-public-methods
    """
    Class to work with Sleep Health Dataset
    """

    def __init__(self, shc_folder="../data/sleephealth/"):
        self.shc_folder = shc_folder


class AboutMeDataset:
    """Provides access to the AboutMe dataset

    Returns
    -------
    Instance of AboutMeDataset
    """

    processed_df = None
    raw_df = None

    def __init__(
        self,
        shc_folder: str = "../data/sleephealth/",
        dataset_filename: str = "About Me.csv",
        pipeline: ProcessingPipeline = ProcessingPipeline(
            [
                DropNAOperator(
                    subset=[
                        "alcohol",
                        "basic_expenses",
                        "caffeine",
                        "daily_activities",
                        "daily_smoking",
                        "education",
                        "flexible_work_hours",
                        "gender",
                        "good_life",
                        "hispanic",
                        "income",
                        "race",
                        "work_schedule",
                        "weight",
                        "smoking_status",
                        "marital",
                    ]
                ),
                DropDuplicatesOperator(subset="participantId", keep="last"),
            ]
        ),
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
        return self.raw_df["participantId"].nunique()

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
        shc_folder: str = "../data/sleephealth/",
        dataset_filename: str = "Sleep Quality Checker.csv",
        pipeline: ProcessingPipeline = ProcessingPipeline(
            [
                SortOperator(by=["participantId", "timestamp"]),
                AggregateOperator(
                    groupby_feature_names="participantId",
                    aggregation_definition={
                        "sq_score": [
                            "count",
                            "mean",
                            "std",
                            "min",
                            "max",
                            "first",
                            "last",
                        ],
                        "timestamp": ["first", "last"],
                    },
                ),
                ConvertToDatetimeOperator(
                    feature_names=["timestamp_last", "timestamp_first"],
                    format="%Y-%m-%dT%H:%M:%S%z",
                    utc=True,
                ),
                CreateFeatureOperator(
                    feature_name="delta_first_last_timestamp",
                    feature_creator=lambda row: row["timestamp_last"]
                    - row["timestamp_first"],
                ),
            ]
        ),
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
        return self.raw_df["participantId"].nunique()

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
        shc_folder: str = "../data/sleephealth/",
        dataset_filename: str = "Onboarding Demographics.csv",
        pipeline: ProcessingPipeline = ProcessingPipeline(
            [ReplaceOperator(to_replace="CENSORED", value=np.nan), DropNAOperator()]
        ),
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
        return self.raw_df["participantId"].nunique()

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
        shc_folder: str = "../data/sleephealth/",
        dataset_filename: str = "Sleep Habits.csv",
        pipeline: ProcessingPipeline = ProcessingPipeline(
            [
                ConvertToDatetimeOperator(
                    feature_names=["timestamp"], format="%Y-%m-%dT%H:%M:%S", utc=True
                ),
                ReplaceOperator(
                    to_replace={
                        "driving_sleepy": {6: np.nan},
                        "morning_person": {3: np.nan},
                        "nap_duration": {6: np.nan},
                        "what_wakes_you": {13: np.nan},
                    }
                ),
                DropNAOperator(
                    subset=[
                        "alarm_dependency",
                        "driving_sleepy",
                        "falling_asleep",
                        "sleep_needed",
                        "sleep_partner",
                        "sleep_time_workday",
                        "wake_up_choices",
                        "wake_ups",
                        "what_wakes_you",
                    ]
                ),
                OneHotEncoderOperator(
                    feature_names=[
                        "alarm_dependency",
                        "driving_sleepy",
                        "falling_asleep",
                        "morning_person",
                        "nap_duration",
                        "sleep_partner",
                        "wake_up_choices",
                        "weekly_naps",
                        "what_wakes_you",
                    ]
                ),
            ]
        ),
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


class MyFamilyDataset:
    """Provides access to the My Family dataset

    Returns
    -------
    Instance of MyFamilyDataset
    """

    def __init__(
        self,
        shc_folder: str = '../data/sleephealth/',
        dataset_filename: str = 'My Family.csv',
        pipeline: ProcessingPipeline = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names="timestamp", format="%Y-%m-%dT%H:%M:%S%z", utc=True),
            SortOperator(by=["participantId", "timestamp"]),
            DropDuplicatesOperator(subset="participantId", keep="last"),
            ReplaceOperator(to_replace={"fam_history": {"200": np.nan},
                                        "family_size": {6: np.nan},
                                        "language": {5: np.nan},
                                        "underage_family": {6: np.nan}}),
            DropNAOperator(subset=["fam_history", "family_size", "language", "underage_family"]),
            OneHotEncoderOperator(feature_names=["fam_history", "family_size", "language", "underage_family"],
                                  drop_first=False),
            DropFeaturesOperator(["fam_history=200"])
        ])
    ):
        """
        My Family Dataset details can be found online at ``https://www.synapse.org/#!Synapse:syn18492837/wiki/593712``.

        Original Shape: (3003, 6)
        Some important stats:
            - This dataset contains 3003 rows for 2760 unique participants.
            - `` participantId `` has 0 NAs ( 3003 / 3003 ) = 0.00 %
            - `` fam_history `` has 15 NAs ( 2988 / 3003 ) = 0.50 %
            - `` family_size `` has 7 NAs ( 2996 / 3003 ) = 0.23 %
            - `` language `` has 4 NAs ( 2999 / 3003 ) = 0.13 %
            - `` underage_family `` has 9 NAs ( 2994 / 3003 ) = 0.30 %
            - `` timestamp `` has 0 NAs ( 3003 / 3003 ) = 0.00 %

        The default pipeline:
            (1) converts timestamp to datatime, sort the dataframe by time and removes all duplicates entries for the
                same participant id, retaining only the last one;
            (2) transforms into NA some values (i.e. answer for a question being "Don't known") in all columns;
            (3) drop NA rows after the transformation above for all columns;
            (4) One hot encode categorical features.
            (5) Drop remaining col for fam_history=200 ("Prefer not to answer"). Apparently some users answered the
                questionnaire AND included the option "prefer not to answer" as well.

        Final dataset shape after default preprocessing pipeline: (2695, 21)
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


class MyHealthDataset:
    """Provides access to the My Health dataset

    Returns
    -------
    Instance of MyHealthDataset
    """

    def __init__(
        self,
        shc_folder: str = '../data/sleephealth/',
        dataset_filename: str = 'My Health.csv',
        pipeline: ProcessingPipeline = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names="timestamp", format="%Y-%m-%dT%H:%M:%S%z", utc=True),
            SortOperator(by=["participantId", "timestamp"]),
            DropDuplicatesOperator(subset="participantId", keep="last"),
            ReplaceOperator(to_replace={"allergies": {3: np.nan}, "anxiety": {3: np.nan}, "apnea": {3: np.nan},
                                        "asthma": {3: np.nan}, "atrial": {3: np.nan}, "hi_blood_pressure": {3: np.nan},
                                        "cancer": {3: np.nan}, "depression": {3: np.nan}, "diabetes": {3: np.nan},
                                        "erectile": {3: np.nan}, "gastroesophageal": {3: np.nan},
                                        "heart_disease": {3: np.nan}, "insomnia": {3: np.nan}, "lung": {3: np.nan},
                                        "narcolepsy": {3: np.nan}, "nocturia": {3: np.nan},
                                        "restless_legs_syndrome": {3: np.nan}, "stroke": {3: np.nan},
                                        "uars": {3: np.nan}
                                        }),
            DropNAOperator(subset=["anxious", "cardiovascular", "compare_one_year", "day_to_day", "depressed",
                                   "emotional", "fatigued", "general_health", "mental_health", "physical_activities",
                                   "physical_health", "risk", "sleep_trouble", "social_activities", "stressed"]),
            OneHotEncoderOperator(feature_names=["anxious", "cardiovascular", "compare_one_year", "day_to_day",
                                                 "depressed", "emotional", "fatigued", "general_health",
                                                 "mental_health", "physical_activities", "physical_health", "risk",
                                                 "sleep_trouble", "social_activities", "stressed"],
                                  drop_first=True),
        ])
    ):
        """
        My Health Dataset details can be found online at ``https://www.synapse.org/#!Synapse:syn18492837/wiki/593707``.
        It contains a set of questions about many diseases.
        For a disease X (X in Allergies, anxiety, apnea, asthma, atrial, hi_blood_pressure, etc.), the following
        questions were made:
            - age_X            ('Age when participant was diagnosed with X')
            - current_X        ('Are you currently being treated for X?')
            - impactsleep_X    ('Does X impact your sleep?'')
            - sleepimpact_X    ('Does sleep impact your X?')

        Question about the general health of the participant were also asked:
            - (compare_one_year) 'What is your general health rating compared to one year ago?'
            - (day_to_day) 'In past 7 days, how much did pain interfere with your day-to-day activities?'
            ... These questions had categorical answers.

        Original Shape: (1551, 114)
        Some important stats:
            - This dataset contains 1551 rows for 1478 unique participants.
            - `` participantId `` has 0 NAs ( 1551 / 1551 ) = 0.00 %
            - `` age_allergies `` has 867 NAs ( 684 / 1551 ) = 55.90 %
            - `` current_allergies `` has 833 NAs ( 718 / 1551 ) = 53.71 %
            - `` impactsleep_allergies `` has 833 NAs ( 718 / 1551 ) = 53.71 %
            - `` sleepimpact_allergies `` has 835 NAs ( 716 / 1551 ) = 53.84 %
            - `` allergies `` has 3 NAs ( 1548 / 1551 ) = 0.19 %
            - `` anxiety `` has 3 NAs ( 1548 / 1551 ) = 0.19 %
            - `` age_anxiety `` has 1094 NAs ( 457 / 1551 ) = 70.54 %
            - `` current_anxiety `` has 1085 NAs ( 466 / 1551 ) = 69.95 %
            - `` impactsleep_anxiety `` has 1085 NAs ( 466 / 1551 ) = 69.95 %
            - `` sleepimpact_anxiety `` has 1084 NAs ( 467 / 1551 ) = 69.89 %
            - `` anxious `` has 1 NAs ( 1550 / 1551 ) = 0.06 %
            - `` apnea `` has 3 NAs ( 1548 / 1551 ) = 0.19 %
            - `` age_apnea `` has 1228 NAs ( 323 / 1551 ) = 79.17 %
            - `` current_apnea `` has 1223 NAs ( 328 / 1551 ) = 78.85 %
            - `` impactsleep_apnea `` has 1223 NAs ( 328 / 1551 ) = 78.85 %
            - `` sleepimpact_apnea `` has 1227 NAs ( 324 / 1551 ) = 79.11 %
            - `` asthma `` has 2 NAs ( 1549 / 1551 ) = 0.13 %
            - `` age_asthma `` has 1242 NAs ( 309 / 1551 ) = 80.08 %
            - `` current_asthma `` has 1239 NAs ( 312 / 1551 ) = 79.88 %
            - `` impactsleep_asthma `` has 1239 NAs ( 312 / 1551 ) = 79.88 %
            - `` sleepimpact_asthma `` has 1239 NAs ( 312 / 1551 ) = 79.88 %
            - `` atrial `` has 0 NAs ( 1551 / 1551 ) = 0.00 %
            - `` age_atrial `` has 1521 NAs ( 30 / 1551 ) = 98.07 %
            - `` current_atrial `` has 1521 NAs ( 30 / 1551 ) = 98.07 %
            - `` impactsleep_atrial `` has 1521 NAs ( 30 / 1551 ) = 98.07 %
            - `` sleepimpact_atrial `` has 1521 NAs ( 30 / 1551 ) = 98.07 %
            - `` hi_blood_pressure `` has 1 NAs ( 1550 / 1551 ) = 0.06 %
            - `` age_hbp `` has 1269 NAs ( 282 / 1551 ) = 81.82 %
            - `` current_hbp `` has 1264 NAs ( 287 / 1551 ) = 81.50 %
            - `` impactsleep_hbp `` has 1264 NAs ( 287 / 1551 ) = 81.50 %
            - `` sleepimpact_hbp `` has 1265 NAs ( 286 / 1551 ) = 81.56 %
            - `` cancer `` has 0 NAs ( 1551 / 1551 ) = 0.00 %
            - `` age_cancer `` has 1486 NAs ( 65 / 1551 ) = 95.81 %
            - `` current_cancer `` has 1486 NAs ( 65 / 1551 ) = 95.81 %
            - `` impactsleep_cancer `` has 1486 NAs ( 65 / 1551 ) = 95.81 %
            - `` sleepimpact_cancer `` has 1486 NAs ( 65 / 1551 ) = 95.81 %
            - `` cardiovascular `` has 7 NAs ( 1544 / 1551 ) = 0.45 %
            - `` compare_one_year `` has 1 NAs ( 1550 / 1551 ) = 0.06 %
            - `` day_to_day `` has 3 NAs ( 1548 / 1551 ) = 0.19 %
            - `` depressed `` has 1 NAs ( 1550 / 1551 ) = 0.06 %
            - `` depression `` has 2 NAs ( 1549 / 1551 ) = 0.13 %
            - `` age_depression `` has 1004 NAs ( 547 / 1551 ) = 64.73 %
            - `` current_depression `` has 997 NAs ( 554 / 1551 ) = 64.28 %
            - `` impactsleep_depression `` has 999 NAs ( 552 / 1551 ) = 64.41 %
            - `` sleepimpact_depression `` has 997 NAs ( 554 / 1551 ) = 64.28 %
            - `` diabetes `` has 0 NAs ( 1551 / 1551 ) = 0.00 %
            - `` age_diabetes `` has 1461 NAs ( 90 / 1551 ) = 94.20 %
            - `` current_Diabetes `` has 1459 NAs ( 92 / 1551 ) = 94.07 %
            - `` impactsleep_diabetes `` has 1459 NAs ( 92 / 1551 ) = 94.07 %
            - `` sleepimpact_diabetes `` has 1460 NAs ( 91 / 1551 ) = 94.13 %
            - `` diabetes_type `` has 1460 NAs ( 91 / 1551 ) = 94.13 %
            - `` emotional `` has 3 NAs ( 1548 / 1551 ) = 0.19 %
            - `` erectile `` has 5 NAs ( 1546 / 1551 ) = 0.32 %
            - `` age_ed `` has 1477 NAs ( 74 / 1551 ) = 95.23 %
            - `` current_ed `` has 1476 NAs ( 75 / 1551 ) = 95.16 %
            - `` impactsleep_ed `` has 1476 NAs ( 75 / 1551 ) = 95.16 %
            - `` sleepimpact_ed `` has 1475 NAs ( 76 / 1551 ) = 95.10 %
            - `` fatigued `` has 3 NAs ( 1548 / 1551 ) = 0.19 %
            - `` gastroesophageal `` has 0 NAs ( 1551 / 1551 ) = 0.00 %
            - `` age_gastroesophageal `` has 1289 NAs ( 262 / 1551 ) = 83.11 %
            - `` current_gastroesophageal `` has 1284 NAs ( 267 / 1551 ) = 82.79 %
            - `` impactsleep_gastroesophageal `` has 1284 NAs ( 267 / 1551 ) = 82.79 %
            - `` sleepimpact_gastroesophageal `` has 1285 NAs ( 266 / 1551 ) = 82.85 %
            - `` general_health `` has 3 NAs ( 1548 / 1551 ) = 0.19 %
            - `` health_care `` has 30 NAs ( 1521 / 1551 ) = 1.93 %
            - `` heart_disease `` has 0 NAs ( 1551 / 1551 ) = 0.00 %
            - `` age_heart_disease `` has 1514 NAs ( 37 / 1551 ) = 97.61 %
            - `` current_heart_disease `` has 1513 NAs ( 38 / 1551 ) = 97.55 %
            - `` impactsleep_heart_disease `` has 1513 NAs ( 38 / 1551 ) = 97.55 %
            - `` sleepimpact_heart_disease `` has 1514 NAs ( 37 / 1551 ) = 97.61 %
            - `` insomnia `` has 1 NAs ( 1550 / 1551 ) = 0.06 %
            - `` age_insomnia `` has 1320 NAs ( 231 / 1551 ) = 85.11 %
            - `` current_insomnia `` has 1313 NAs ( 238 / 1551 ) = 84.66 %
            - `` impactsleep_insomnia `` has 1313 NAs ( 238 / 1551 ) = 84.66 %
            - `` sleepimpact_insomnia `` has 1313 NAs ( 238 / 1551 ) = 84.66 %
            - `` lung `` has 0 NAs ( 1551 / 1551 ) = 0.00 %
            - `` age_lung `` has 1535 NAs ( 16 / 1551 ) = 98.97 %
            - `` current_lung `` has 1535 NAs ( 16 / 1551 ) = 98.97 %
            - `` impactsleep_lung `` has 1535 NAs ( 16 / 1551 ) = 98.97 %
            - `` sleepimpact_lung `` has 1535 NAs ( 16 / 1551 ) = 98.97 %
            - `` mental_health `` has 0 NAs ( 1551 / 1551 ) = 0.00 %
            - `` narcolepsy `` has 2 NAs ( 1549 / 1551 ) = 0.13 %
            - `` age_Narcolepsy `` has 1516 NAs ( 35 / 1551 ) = 97.74 %
            - `` current_Narcolepsy `` has 1516 NAs ( 35 / 1551 ) = 97.74 %
            - `` impactsleep_Narcolepsy `` has 1515 NAs ( 36 / 1551 ) = 97.68 %
            - `` sleepimpact_Narcolepsy `` has 1515 NAs ( 36 / 1551 ) = 97.68 %
            - `` nocturia `` has 5 NAs ( 1546 / 1551 ) = 0.32 %
            - `` age_Nocturia `` has 1515 NAs ( 36 / 1551 ) = 97.68 %
            - `` current_Nocturia `` has 1515 NAs ( 36 / 1551 ) = 97.68 %
            - `` impactsleep_Nocturia `` has 1515 NAs ( 36 / 1551 ) = 97.68 %
            - `` sleepimpact_Nocturia `` has 1515 NAs ( 36 / 1551 ) = 97.68 %
            - `` restless_legs_syndrome `` has 2 NAs ( 1549 / 1551 ) = 0.13 %
            - `` age_rls `` has 1444 NAs ( 107 / 1551 ) = 93.10 %
            - `` current_rls `` has 1442 NAs ( 109 / 1551 ) = 92.97 %
            - `` impactsleep_rls `` has 1442 NAs ( 109 / 1551 ) = 92.97 %
            - `` sleepimpact_rls `` has 1443 NAs ( 108 / 1551 ) = 93.04 %
            - `` stroke `` has 1 NAs ( 1550 / 1551 ) = 0.06 %
            - `` age_Stroke `` has 1530 NAs ( 21 / 1551 ) = 98.65 %
            - `` current_Stroke `` has 1530 NAs ( 21 / 1551 ) = 98.65 %
            - `` impactsleep_stroke `` has 1530 NAs ( 21 / 1551 ) = 98.65 %
            - `` sleepimpact_stroke `` has 1530 NAs ( 21 / 1551 ) = 98.65 %
            - `` physical_activities `` has 2 NAs ( 1549 / 1551 ) = 0.13 %
            - `` physical_health `` has 3 NAs ( 1548 / 1551 ) = 0.19 %
            - `` risk `` has 5 NAs ( 1546 / 1551 ) = 0.32 %
            - `` sleep_trouble `` has 8 NAs ( 1543 / 1551 ) = 0.52 %
            - `` social_activities `` has 2 NAs ( 1549 / 1551 ) = 0.13 %
            - `` stressed `` has 0 NAs ( 1551 / 1551 ) = 0.00 %
            - `` uars `` has 2 NAs ( 1549 / 1551 ) = 0.13 %
            - `` age_uars `` has 1546 NAs ( 5 / 1551 ) = 99.68 %
            - `` current_uars `` has 1545 NAs ( 6 / 1551 ) = 99.61 %
            - `` impactsleep_uars `` has 1545 NAs ( 6 / 1551 ) = 99.61 %
            - `` sleepimpact_uars `` has 1545 NAs ( 6 / 1551 ) = 99.61 %
            - `` timestamp `` has 0 NAs ( 1551 / 1551 ) = 0.00 %

        The default pipeline:
            (1) converts timestamp to datatime, sort the dataframe by time and removes all duplicates entries for the
                same participant id, retaining only the last one;
            (2) transforms into NA some values (i.e. answer for a question being "Don't known") in all columns;
            (3) Left the disease specific NA rows, but dropped NAs in the general health questions
            (4) One hot encode the general health questions

        Final dataset shape after default preprocessing pipeline: (1445, 160)
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


class ResearchInterestDataset:
    """Provides access to the Research Interest dataset

    Returns
    -------
    Instance of ResearchInterestDataset
    """

    def __init__(
        self,
        shc_folder: str = '../data/sleephealth/',
        dataset_filename: str = 'Research Interest.csv',
        pipeline: ProcessingPipeline = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names="timestamp", format="%Y-%m-%dT%H:%M:%S%z", utc=True),
            SortOperator(by=["participantId", "timestamp"]),
            DropDuplicatesOperator(subset="participantId", keep="last"),
            ReplaceOperator(to_replace={"research_experience": {3: np.nan}}),
            DropNAOperator(subset=['contact_method', 'research_experience', 'two_surveys_perday',
                                   'blood_sample', 'taking_medication', 'family_survey', 'hospital_stay']),
            OneHotEncoderOperator(feature_names=['contact_method', 'research_experience', 'two_surveys_perday',
                                                 'blood_sample', 'taking_medication', 'family_survey', 'hospital_stay'],
                                  drop_first=True),
        ])
    ):
        """
        Research Interest details can be found online at ``https://www.synapse.org/#!Synapse:syn18492837/wiki/593711``.

        Original Shape: (2359, 9)
        Some important stats:
            - This dataset contains 1551 rows for 1478 unique participants.
            - `` participantId `` has 0 NAs ( 2359 / 2359 ) = 0.00 %
            - `` contact_method `` has 8 NAs ( 2351 / 2359 ) = 0.34 %
            - `` research_experience `` has 4 NAs ( 2355 / 2359 ) = 0.17 %
            - `` two_surveys_perday `` has 8 NAs ( 2351 / 2359 ) = 0.34 %
            - `` blood_sample `` has 6 NAs ( 2353 / 2359 ) = 0.25 %
            - `` taking_medication `` has 7 NAs ( 2352 / 2359 ) = 0.30 %
            - `` family_survey `` has 10 NAs ( 2349 / 2359 ) = 0.42 %
            - `` hospital_stay `` has 11 NAs ( 2348 / 2359 ) = 0.47 %
            - `` timestamp `` has 0 NAs ( 2359 / 2359 ) = 0.00 %


        The default pipeline:
            (1) converts timestamp to datatime, sort the dataframe by time and removes all duplicates entries for the
                same participant id, retaining only the last one;
            (2) transforms into NA some values (i.e. answer for a question being "Don't known") in all columns;
            (3) drop NA rows after the transformation above for all columns;
            (4) One hot encode categorical features.

        Final dataset shape after default preprocessing pipeline: (2072, 21)
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


class SleepAssessmentDataset:
    """Provides access to the Sleep Assessment dataset

    Returns
    -------
    Instance of SleepAssessmentDataset
    """

    def __init__(
        self,
        shc_folder: str = '../data/sleephealth/',
        dataset_filename: str = 'Sleep Assessment.csv',
        pipeline: ProcessingPipeline = ProcessingPipeline([
            ConvertToDatetimeOperator(feature_names="timestamp", format="%Y-%m-%dT%H:%M:%S%z", utc=True),
            SortOperator(by=["participantId", "timestamp"]),
            DropDuplicatesOperator(subset="participantId", keep="last"),
            ReplaceOperator(to_replace={"alcohol": {7: np.nan},
                                        "medication_by_doctor": {7: np.nan},
                                        "sleep_aids": {7: np.nan},
                                        "told_by_doctor": {3: np.nan},
                                        "told_to_doctor": {3: np.nan},
                                        "told_by_doctor_specify": {np.nan: '8'},
                                        "other_selected": {np.nan: ''},
                                        }),
            DropNAOperator(subset=['alcohol', 'concentrating_problem_one', 'concentrating_problem_two',
                                   'discomfort_in_sleep', 'exercise', 'fatigue_limit', 'feel_tired_frequency',
                                   'felt_alert', 'had_problem', 'hard_times', 'medication_by_doctor',
                                   'poor_sleep_problems', 'sleep_aids', 'sleep_problem', 'think_clearly',
                                   'tired_easily', 'told_by_doctor', 'told_to_doctor', 'trouble_staying_awake']),
            OneHotEncoderOperator(feature_names=['alcohol', 'concentrating_problem_one', 'concentrating_problem_two',
                                                 'discomfort_in_sleep', 'exercise', 'fatigue_limit',
                                                 'feel_tired_frequency', 'felt_alert', 'had_problem', 'hard_times',
                                                 'medication_by_doctor', 'poor_sleep_problems', 'sleep_aids',
                                                 'sleep_problem', 'think_clearly', 'tired_easily', 'told_by_doctor',
                                                 'told_to_doctor', 'trouble_staying_awake', 'told_by_doctor_specify'],
                                  drop_first=True),
        ])
    ):
        """
        Sleep Assessment Dataset details can be found online at ``https://www.synapse.org/#!Synapse:syn18492837/wiki/593721``.

        Original Shape: (2325, 23)
        Some important stats:
            - This dataset contains 2325 rows for 2228 unique participants.
            - `` participantId `` has 0 NAs ( 2325 / 2325 ) = 0.00 %
            - `` alcohol `` has 2 NAs ( 2323 / 2325 ) = 0.09 %
            - `` concentrating_problem_one `` has 4 NAs ( 2321 / 2325 ) = 0.17 %
            - `` concentrating_problem_two `` has 4 NAs ( 2321 / 2325 ) = 0.17 %
            - `` discomfort_in_sleep `` has 7 NAs ( 2318 / 2325 ) = 0.30 %
            - `` exercise `` has 30 NAs ( 2295 / 2325 ) = 1.29 %
            - `` fatigue_limit `` has 9 NAs ( 2316 / 2325 ) = 0.39 %
            - `` feel_tired_frequency `` has 1 NAs ( 2324 / 2325 ) = 0.04 %
            - `` felt_alert `` has 1 NAs ( 2324 / 2325 ) = 0.04 %
            - `` had_problem `` has 6 NAs ( 2319 / 2325 ) = 0.26 %
            - `` hard_times `` has 3 NAs ( 2322 / 2325 ) = 0.13 %
            - `` medication_by_doctor `` has 4 NAs ( 2321 / 2325 ) = 0.17 %
            - `` poor_sleep_problems `` has 0 NAs ( 2325 / 2325 ) = 0.00 %
            - `` sleep_aids `` has 4 NAs ( 2321 / 2325 ) = 0.17 %
            - `` sleep_problem `` has 8 NAs ( 2317 / 2325 ) = 0.34 %
            - `` think_clearly `` has 3 NAs ( 2322 / 2325 ) = 0.13 %
            - `` tired_easily `` has 2 NAs ( 2323 / 2325 ) = 0.09 %
            - `` told_by_doctor `` has 9 NAs ( 2316 / 2325 ) = 0.39 %
            - `` told_by_doctor_specify `` has 1642 NAs ( 683 / 2325 ) = 70.62 %
            - `` told_to_doctor `` has 1 NAs ( 2324 / 2325 ) = 0.04 %
            - `` other_selected `` has 2249 NAs ( 76 / 2325 ) = 96.73 %
            - `` trouble_staying_awake `` has 3 NAs ( 2322 / 2325 ) = 0.13 %
            - `` timestamp `` has 0 NAs ( 2325 / 2325 ) = 0.00 %


        The default pipeline:
            (1) converts timestamp to datatime, sort the dataframe by time and removes all duplicates entries for the
                same participant id, retaining only the last one;
            (2) transforms into NA some values (i.e. answer for a question being "Don't known") in all columns;
            (3) drop NA rows after the transformation above for all columns;
            (4) One hot encode categorical features;
            (5) Dataset contains one string column ('told_by_doctor_specify'), with text that was not preprocessed.

        Final dataset shape after default preprocessing pipeline: (2123, 83)
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
        return self.raw_df["participantId"].nunique()

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
