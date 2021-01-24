import pathlib

import pandas as pd


class MyHeartCountsDataset:

    '''
    Class to work with Standford meds public dataset MyHeartCounts
    '''

    def __init__(self,
                 mhc_folder='~/Documents/Data/MyHeartCounts'):
        self.mhc_folder = mhc_folder



class DailyCheckSurveyDataset:

    dcs_df = None
    raw_df = None

    def __init__(
        self,
        mhc_folder='~/Documents/Data/MyHeartCounts',
        dcs_filename='Daily Check Survey.csv',
        merge_activity_features = False,
        drop_features=['appVersion', 'phoneInfo', 'activity1_type', 'activity2_type', 'phone_on_user']):

        full_path = pathlib.Path(mhc_folder, dcs_filename)
        self.dcs_df = pd.read_csv(full_path)
        self.raw_df = self.dcs_df.copy()
        self.act1 = 'activity1_option'
        self.act1_t = 'activity1_time'
        self.act1_i = 'activity1_intensity'
        self.act2 = 'activity2_option'
        self.act2_t = 'activity2_time'
        self.act2_i = 'activity2_intensity'
        self.act_i = 'activity_intensity'
        self.act_t = 'activity_time'
        self.merge_activity_features = merge_activity_features
        self.drop_features = drop_features
        self._process()
        self._group()


    def participant_count(self):

        n = self.raw_df['healthCode'].nunique()
        return n

    def raw_dataframe(self):
        return self.raw_df

    def processed_dataframe(self):
        return self.dcs_df

    def grouped_dataframe(self):
        return self.group_dcs_df

    def _process(self):

        self.dcs_df = self.dcs_df.drop(self.drop_features, axis=1)

        #number of patients with act1 or act2 data
        self.dcs_df.loc[self.dcs_df[self.act1].isnull() & self.dcs_df[self.act2].notnull(), self.act1] = False
        self.dcs_df.loc[self.dcs_df[self.act2].isnull() & self.dcs_df[self.act1].notnull(), self.act2] = False


        # Set intensity value to 4 where there was activity1 performed and minutes recorded but intensity omitted
        self.dcs_df.loc[pd.notnull(self.dcs_df[self.act1]) & self.dcs_df[self.act1] & pd.notnull(self.dcs_df[self.act1_t]) & pd.isnull(self.dcs_df[self.act1_i]), self.act1_i] = 4
        # Set activity1 values to zero when activity1 was not performed
        self.dcs_df.loc[pd.notnull(self.dcs_df[self.act1]) & self.dcs_df[self.act1] == False, [self.act1_i, self.act1_t]] = 0
        # Set intensity value to 4 where there was activity1 performed and minutes recorded but intensity omitted
        self.dcs_df.loc[pd.notnull(self.dcs_df[self.act2]) & self.dcs_df[self.act2] & pd.notnull(self.dcs_df[self.act2_t]) & pd.isnull(self.dcs_df[self.act2_i]), self.act2_i] = 4
        # Set activity1 values to zero when activity1 was not performed
        self.dcs_df.loc[pd.notnull(self.dcs_df[self.act2]) & self.dcs_df[self.act2] == False, [self.act2_i, self.act2_t]] = 0

        self.dcs_df = self.dcs_df.drop([self.act1, self.act2], axis=1)

        if self.merge_activity_features:
            self.dcs_df[self.act_i] = (self.dcs_df[self.act1_i] + self.dcs_df[self.act2_i])/2
            self.dcs_df[self.act_t] = (self.dcs_df[self.act1_t] + self.dcs_df[self.act2_t])/2



    def _group(self):
        stats = ['mean', 'std']
        group_agg = {self.act1_i : stats, self.act1_t: stats, self.act2_i: stats, self.act2_t: stats, 'sleep_time': stats}
        if self.merge_activity_features:
            group_agg = {self.act_i : stats, self.act_t: stats, 'sleep_time': stats}

        self.group_dcs_df = self.dcs_df.drop('recordId', axis=1).copy()
        self.group_dcs_df = self.group_dcs_df.groupby('healthCode', as_index=False).agg(group_agg)

        if self.merge_activity_features:
            self.group_dcs_df.columns = ['healthCode', self.act_i+"_mean", self.act_i+"_std", self.act_t+"_mean", self.act_t+"_std", 'sleep_time_mean', 'sleep_time_std']
        else:
            self.group_dcs_df.columns = ['healthCode', self.act1_i+"_mean", self.act1_i+"_std", self.act1_t+"_mean", self.act1_t+"_std", self.act2_i+"_mean", self.act2_i+"_std", self.act2_t+"_mean", self.act2_t+"_std", 'sleep_time_mean', 'sleep_time_std']


class DayOneSurveyDataset:

    dos_df = None
    raw_df = None

    device_mapping = {
        "iPhone": "1",
        "ActivityBand": "2",
        "Pedometer": "2",
        "SmartWatch": "3",
        "AppleWatch": "3",
        "Other": "Other"
    }

    def __init__(
        self,
        mhc_folder='~/Documents/Data/MyHeartCounts',
        dos_filename='Day One Survey.csv',
        drop_device_na = False,
        drop_labwork_na = False):

        full_path = pathlib.Path(mhc_folder, dos_filename)
        self.dos_df = pd.read_csv(full_path)
        self.raw_df = self.dos_df.copy()
        self.drop_device_na = drop_device_na
        self.drop_labwork_na = drop_labwork_na
        self._process()


    def participant_count(self):

        n = self.raw_df['healthCode'].nunique()
        return n

    def raw_dataframe(self):
        return self.raw_df

    def processed_dataframe(self):

        return self.dos_df


    def _process(self):

        if self.drop_device_na and self.drop_labwork_na:
            self.dos_df = self.dos_df[(self.dos_df.device.notnull()) & (self.dos_df.labwork.notnull())].copy()

        if self.drop_device_na:
            self.dos_df = self.dos_df[self.dos_df.device.notnull()].copy()

        if self.drop_labwork_na:
            self.dos_df = self.dos_df[self.dos_df.labwork.notnull()].copy()

class PARQSurveyDataset:

    parq_df = None
    raw_df = None

    def __init__(
        self,
        mhc_folder='~/Documents/Data/MyHeartCounts',
        dos_filename='PAR-Q Survey.csv',
        drop_unanswered = True,
        drop_duplicates = True):

        full_path = pathlib.Path(mhc_folder, dos_filename)
        self.parq_df = pd.read_csv(full_path)
        self.raw_df = self.parq_df.copy()
        self.drop_unanswered = drop_unanswered
        self.drop_duplicates = drop_duplicates
        self._process()


    def participant_count(self):

        n = self.raw_df['healthCode'].nunique()
        return n

    def raw_dataframe(self):
        return self.raw_df

    def processed_dataframe(self):

        return self.parq_df


    def _process(self):

        if self.drop_unanswered:
            self.parq_df = self.parq_df[
                self.parq_df.chestPain.notnull() &
                self.parq_df.chestPainInLastMonth.notnull() &
                self.parq_df.dizziness.notnull() &
                self.parq_df.heartCondition.notnull() &
                self.parq_df.jointProblem.notnull() &
                self.parq_df.physicallyCapable.notnull() &
                self.parq_df.prescriptionDrugs.notnull()]

        if self.drop_duplicates:
            self.parq_df = self.parq_df.drop_duplicates(subset='healthCode', keep='last')

class RiskFactorSurvey:

    rf_df = None
    raw_df = None

    def __init__(
        self,
        mhc_folder='~/Documents/Data/MyHeartCounts',
        rfs_filename='Risk Factor Survey.csv',
        drop_features=['family_history', 'medications_to_treat', 'heart_disease', 'vascular', 'ethnicity', 'race', 'education']):

        full_path = pathlib.Path(mhc_folder, rfs_filename)
        self.rf_df = pd.read_csv(full_path)
        self.raw_df = self.rf_df.copy()
        self.drop_features = drop_features
        self._process()


    def participant_count(self):
        n = self.raw_df['healthCode'].nunique()
        return n

    def raw_dataframe(self):
        return self.raw_df

    def processed_dataframe(self):
        return self.rf_df


    def _process(self):
        for c in self.drop_features:
            if c not in self.rf_df.columns:
                raise ValueError(str(c) + ' not in columns')

        self.rf_df = self.rf_df.drop(self.drop_features, axis=1)

class CardioDietSurveyDataset:

    cd_df = None
    raw_df = None

    def __init__(
        self,
        mhc_folder='~/Documents/Data/MyHeartCounts',
        cds_filename='Cardio Diet Survey.csv',
        drop_features=[]):

        full_path = pathlib.Path(mhc_folder, cds_filename)
        self.cd_df = pd.read_csv(full_path)
        self.raw_df = self.cd_df.copy()
        self.drop_features = drop_features
        self._process()


    def participant_count(self):
        n = self.raw_df['healthCode'].nunique()
        return n

    def raw_dataframe(self):
        return self.raw_df

    def processed_dataframe(self):
        return self.cd_df

    def _process(self):
        for c in self.drop_features:
            if c not in self.cd_df.columns:
                raise ValueError(str(c) + ' not in columns')

        self.cd_df = self.cd_df.drop(self.drop_features, axis=1)
