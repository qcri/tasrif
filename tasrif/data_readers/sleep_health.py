import pathlib
import pandas as pd


class SleepHealthDataset:
    """
    Class to work with Sleep Health Dataset
    """

    def __init__(self,
                 shc_folder='../data/sleephealth/'):
        self.shc_folder = shc_folder
        # TODO: placeholder only, missing implementation
        pass


class AboutMeDataset:
    aboutme_df = None
    raw_df = None

    def __init__(
            self,
            shc_folder: str ='../data/sleephealth/',
            amd_filename: str ='About Me.csv',
            drop_unanswered: bool = True,
            drop_duplicates: bool = True):
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

            By using drop_unanswered == True, we remove NAs for participants in all columns,
            but ``menopause``, ``recent_births`` and ``current_pregnant``, resulting in a dataset with 3181 participants.

            There are duplicates in the dataset. If drop_duplicates == True, the dataset size decreases from 3448 to 3262.

            Finally, if drop_duplicates == True and drop_unanswered == True (default), the final dataset size is 3019.

        """

        full_path = pathlib.Path(shc_folder, amd_filename)
        self.aboutme_df = pd.read_csv(full_path)
        self.raw_df = self.aboutme_df.copy()
        self.drop_unanswered = drop_unanswered
        self.drop_duplicates = drop_duplicates
        self._process()

    def participant_count(self):
        n = self.raw_df['participantId'].nunique()
        return n

    def raw_dataframe(self):
        return self.raw_df

    def processed_dataframe(self):
        return self.aboutme_df

    def _process(self):
        if self.drop_unanswered:
            self.aboutme_df = self.aboutme_df[self.aboutme_df.alcohol.notnull() &
                                              self.aboutme_df.basic_expenses.notnull() &
                                              self.aboutme_df.caffeine.notnull() &
                                              self.aboutme_df.daily_activities.notnull() &
                                              self.aboutme_df.daily_smoking.notnull() &
                                              self.aboutme_df.education.notnull() &
                                              self.aboutme_df.flexible_work_hours.notnull() &
                                              self.aboutme_df.gender.notnull() &
                                              self.aboutme_df.good_life.notnull() &
                                              self.aboutme_df.hispanic.notnull() &
                                              self.aboutme_df.income.notnull() &
                                              self.aboutme_df.marital.notnull() &
                                              self.aboutme_df.race.notnull() &
                                              self.aboutme_df.smoking_status.notnull() &
                                              self.aboutme_df.weight.notnull() &
                                              # Decided not to remove the following 3 cols by default:
                                              # self.aboutme_df.menopause.notnull() &
                                              # self.aboutme_df.recent_births.notnull() &
                                              # self.aboutme_df.current_pregnant.notnull() &
                                              self.aboutme_df.work_schedule.notnull()
                                              ]

        if self.drop_duplicates:
            self.aboutme_df = self.aboutme_df.drop_duplicates(subset='participantId', keep='last')
