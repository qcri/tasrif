"""Module that provides classes to access sleep health datasets.
For details please read https://www.nature.com/articles/s41597-020-00753-2
"""
import pathlib
import pandas as pd

from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.pandas import DropNAOperator
from tasrif.processing_pipeline.pandas import DropDuplicatesOperator


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
                 amd_filename: str = 'About Me.csv',
                 processing_pipeline: ProcessingPipeline = ProcessingPipeline(
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

        full_path = pathlib.Path(shc_folder, amd_filename)
        self.raw_df = pd.read_csv(full_path)
        self.processed_df = self.raw_df.copy()
        self.processing_pipeline = processing_pipeline
        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df['participantId'].nunique()
        return number_participants

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
        if self.processing_pipeline:
            self.processed_df = self.processing_pipeline.process(self.processed_df)[0]
