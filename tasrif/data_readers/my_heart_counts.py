"""Module that provides classes to work with the MyHeartCounts dataset
   Available datasets:
        MyHeartCountsDataset
        DailyCheckSurveyDataset
        DayOneSurveyDataset
        PARQSurveyDataset
        RiskFactorSurvey
        CardioDietSurveyDataset
"""
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
    """Class to work with the Daily Survey Table in the MyHeartCounts dataset.

    """

    class Defaults: #pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        """
        DROP_FEATURES = ['appVersion', 'phoneInfo', 'activity1_type', 'activity2_type', 'phone_on_user']

    ACT1 = 'activity1_option'
    ACT1_T = 'activity1_time'
    ACT1_I = 'activity1_intensity'
    ACT2 = 'activity2_option'
    ACT2_T = 'activity2_time'
    ACT2_I = 'activity2_intensity'
    ACT_I = 'activity_intensity'
    ACT_T = 'activity_time'

    def __init__(self,\
        mhc_folder='~/Documents/Data/MyHeartCounts',\
        dcs_filename='Daily Check Survey.csv',\
        merge_activity_features=False,\
        drop_features=Defaults.DROP_FEATURES):

        full_path = pathlib.Path(mhc_folder, dcs_filename)
        self.dcs_df = pd.read_csv(full_path)
        self.raw_df = self.dcs_df.copy()
        self.merge_activity_features = merge_activity_features
        self.drop_features = drop_features
        self._process()
        self._group()


    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df['healthCode'].nunique()
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

        return self.dcs_df

    def grouped_dataframe(self):
        """Gets the dataframe grouped by participants. The result is a data frames where each row
        represents exactly one partipant

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.group_dcs_df

    def _process(self):
        """Modifies self.dcs_df by dropping unnecessary columns (features), 
        filling null activity intensity,
        filling null activity time values,
        and averages activity1 and activity2 if given the option.


        Pseudocode:
            - start with raw dataframe dcs_df
            - drop self.drop_features features from dcs_df 
            - set activity1_option null values to false if activity2_option is filled
            - set activity1_intensity to 4 if activity1_option and activity1_time are filled
            - set activity1_intensity and activity1_time to 0 if activity1_option is false
            - repeat the three steps above for activity2_option
            - drop activity1_option and activity2_option from dcs_df
            - average the intensities if self.merge_activity_features is true, set it to column ACT_I
            - average the time if self.merge_activity_features is true, set it to column ACT_T
            - return dcs_df
        Returns
        -------
        sets the result in self.dcs_df
        """

        self.dcs_df = self.dcs_df.drop(self.drop_features, axis=1)

        #number of patients with act1 or act2 data
        self.dcs_df.loc[self.dcs_df[DailyCheckSurveyDataset.ACT1].isnull() &\
                        self.dcs_df[DailyCheckSurveyDataset.ACT2].notnull(),\
                        DailyCheckSurveyDataset.ACT1] = False
        self.dcs_df.loc[self.dcs_df[DailyCheckSurveyDataset.ACT2].isnull() &\
                        self.dcs_df[DailyCheckSurveyDataset.ACT1].notnull(),\
                        DailyCheckSurveyDataset.ACT2] = False


        # Set intensity value to 4 where there was activity1 performed and minutes recorded but intensity omitted
        self.dcs_df.loc[pd.notnull(self.dcs_df[DailyCheckSurveyDataset.ACT1]) & \
                        self.dcs_df[DailyCheckSurveyDataset.ACT1] & \
                        pd.notnull(self.dcs_df[DailyCheckSurveyDataset.ACT1_T]) & \
                        pd.isnull(self.dcs_df[DailyCheckSurveyDataset.ACT1_I]),
                        DailyCheckSurveyDataset.ACT1_I] = 4
        # Set activity1 values to zero when activity1 was not performed
        self.dcs_df.loc[pd.notnull(self.dcs_df[DailyCheckSurveyDataset.ACT1]) & self.dcs_df[DailyCheckSurveyDataset.ACT1] == False, #pylint: disable = singleton-comparison
                        [DailyCheckSurveyDataset.ACT1_I, DailyCheckSurveyDataset.ACT1_T]] = 0
        # Set intensity value to 4 where there was activity1 performed and minutes recorded but intensity omitted
        self.dcs_df.loc[pd.notnull(self.dcs_df[DailyCheckSurveyDataset.ACT2]) & \
          self.dcs_df[DailyCheckSurveyDataset.ACT2] & \
            pd.notnull(self.dcs_df[DailyCheckSurveyDataset.ACT2_T]) & pd.isnull(self.dcs_df[DailyCheckSurveyDataset.ACT2_I]), \
            DailyCheckSurveyDataset.ACT2_I] = 4
        # Set activity2 values to zero when activity2 was not performed
        self.dcs_df.loc[pd.notnull(self.dcs_df[DailyCheckSurveyDataset.ACT2]) & self.dcs_df[DailyCheckSurveyDataset.ACT2] == False, #pylint: disable = singleton-comparison
                        [DailyCheckSurveyDataset.ACT2_I, DailyCheckSurveyDataset.ACT2_T]] = 0

        self.dcs_df = self.dcs_df.drop([DailyCheckSurveyDataset.ACT1, DailyCheckSurveyDataset.ACT2], axis=1)

        if self.merge_activity_features:
            self.dcs_df[DailyCheckSurveyDataset.ACT_I] = (self.dcs_df[DailyCheckSurveyDataset.ACT1_I] + self.dcs_df[DailyCheckSurveyDataset.ACT2_I])/2
            self.dcs_df[DailyCheckSurveyDataset.ACT_T] = (self.dcs_df[DailyCheckSurveyDataset.ACT1_T] + self.dcs_df[DailyCheckSurveyDataset.ACT2_T])/2



    def _group(self):
        """Modifies self.dcs_df by dropping unnecessary columns (features), 
        filling null activity intensity,
        filling null activity time values,
        and averages activity1 and activity2 if given the option.


        Pseudocode:
            - start with self.dcs_df
            - for each paricipant (represented by healthCode column) in self.dcs_df
                - get the mean of activity1_intensity, activity1_time
                - get the standard deviation of activity1_intensity, activity1_time
                - repeat for activity2_option
            - if self.merge_activity_features, then do the above with activity_intensity, and activity_time instead
            - set the result in self.group_dcs_df
        Returns
        -------
        sets the result in self.group_dcs_df
        """
        stats = ['mean', 'std']
        group_agg = {DailyCheckSurveyDataset.ACT1_I : stats,\
                     DailyCheckSurveyDataset.ACT1_T : stats,\
                     DailyCheckSurveyDataset.ACT2_I : stats,\
                     DailyCheckSurveyDataset.ACT2_T : stats,\
                     'sleep_time' : stats}
        if self.merge_activity_features:
            group_agg = {DailyCheckSurveyDataset.ACT_I : stats, DailyCheckSurveyDataset.ACT_T: stats, 'sleep_time': stats}

        self.group_dcs_df = self.dcs_df.drop('recordId', axis=1).copy()
        self.group_dcs_df = self.group_dcs_df.groupby('healthCode', as_index=False).agg(group_agg)

        if self.merge_activity_features:
            self.group_dcs_df.columns = [
                'healthCode',
                DailyCheckSurveyDataset.ACT_I+"_mean",\
                DailyCheckSurveyDataset.ACT_I+"_std",\
                DailyCheckSurveyDataset.ACT_T+"_mean",\
                DailyCheckSurveyDataset.ACT_T+"_std",
                'sleep_time_mean',\
                'sleep_time_std']
        else:
            self.group_dcs_df.columns = [
                'healthCode',\
                DailyCheckSurveyDataset.ACT1_I+"_mean",\
                DailyCheckSurveyDataset.ACT1_I+"_std",\
                DailyCheckSurveyDataset.ACT1_T+"_mean",\
                DailyCheckSurveyDataset.ACT1_T+"_std",\
                DailyCheckSurveyDataset.ACT2_I+"_mean",\
                DailyCheckSurveyDataset.ACT2_I+"_std",\
                DailyCheckSurveyDataset.ACT2_T+"_mean",
                DailyCheckSurveyDataset.ACT2_T+"_std", \
                'sleep_time_mean',\
                'sleep_time_std']


class DayOneSurveyDataset:
    """Class to work with the Day One Survey Table in the MyHeartCounts dataset.

    """
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

    def __init__(self,\
        mhc_folder='~/Documents/Data/MyHeartCounts',\
        dos_filename='Day One Survey.csv',\
        drop_device_na=False,\
        drop_labwork_na=False):

        full_path = pathlib.Path(mhc_folder, dos_filename)
        self.dos_df = pd.read_csv(full_path)
        self.raw_df = self.dos_df.copy()
        self.drop_device_na = drop_device_na
        self.drop_labwork_na = drop_labwork_na
        self._process()


    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df['healthCode'].nunique()
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

        return self.dos_df


    def _process(self):
        """Modifies self.dos_df by dropping na columns given bools,
        self.drop_device_na and self.drop_labwork_na 
        
        Returns
        -------
        sets the result in self.dos_df
        """

        if self.drop_device_na and self.drop_labwork_na:
            self.dos_df = self.dos_df[(self.dos_df.device.notnull()) & (self.dos_df.labwork.notnull())].copy()

        if self.drop_device_na:
            self.dos_df = self.dos_df[self.dos_df.device.notnull()].copy()

        if self.drop_labwork_na:
            self.dos_df = self.dos_df[self.dos_df.labwork.notnull()].copy()

class PARQSurveyDataset:
    """Class to work with the PARQ Survey Table in the MyHeartCounts dataset.

    """

    parq_df = None
    raw_df = None

    def __init__(self,\
        mhc_folder='~/Documents/Data/MyHeartCounts',\
        dos_filename='PAR-Q Survey.csv',\
        drop_unanswered=True,\
        drop_duplicates=True):

        full_path = pathlib.Path(mhc_folder, dos_filename)
        self.parq_df = pd.read_csv(full_path)
        self.raw_df = self.parq_df.copy()
        self.drop_unanswered = drop_unanswered
        self.drop_duplicates = drop_duplicates
        self._process()


    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df['healthCode'].nunique()
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

        return self.parq_df


    def _process(self):
        """Modifies self.parq_df by dropping any row that contains 
        a null value. drops duplicate rows if self.drop_duplicates
        is true
        
        Returns
        -------
        sets the result in self.parq_df
        """        

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
    """Class to work with the Risk factor Survey Table in the MyHeartCounts dataset.

    """

    class Default:#pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        """
        DROP_FEATURES = ['family_history', 'medications_to_treat', 'heart_disease', 'vascular', 'ethnicity', 'race', 'education']

    def __init__(self,\
        mhc_folder='~/Documents/Data/MyHeartCounts',\
        rfs_filename='Risk Factor Survey.csv',\
        drop_features=Default.DROP_FEATURES):

        full_path = pathlib.Path(mhc_folder, rfs_filename)
        self.rf_df = pd.read_csv(full_path)
        self.raw_df = self.rf_df.copy()
        self.drop_features = drop_features
        self._process()


    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df['healthCode'].nunique()
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

        return self.rf_df


    def _process(self):
        """Modifies self.rf_df by dropping columns (features) that
        are given in self.drop_features 

        Raises
        -------
        ValueError if self.drop_features contain a non existent column
        within self.rf_df
        
        Returns
        -------
        sets the result in self.rf_df
        """        

        for feature in self.drop_features:
            if feature not in self.rf_df.columns:
                raise ValueError(str(feature) + ' not in columns')

        self.rf_df = self.rf_df.drop(self.drop_features, axis=1)

class CardioDietSurveyDataset:
    """Class to work with the Cardio diet survey Table in the MyHeartCounts dataset.

    """

    class Default:#pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        """
        DROP_FEATURES = []

    def __init__(self,\
        mhc_folder='~/Documents/Data/MyHeartCounts',\
        cds_filename='Cardio Diet Survey.csv',\
        drop_features=Default.DROP_FEATURES):

        full_path = pathlib.Path(mhc_folder, cds_filename)
        self.cd_df = pd.read_csv(full_path)
        self.raw_df = self.cd_df.copy()
        self.drop_features = drop_features
        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df['healthCode'].nunique()
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

        return self.cd_df

    def _process(self):
        """Modifies self.cd_df by dropping columns (features) that
        are given in self.drop_features 

        Raises
        -------
        ValueError if self.drop_features contain a non existent column
        within self.cd_df
        
        Returns
        -------
        sets the result in self.cd_df
        """  

        for feature in self.drop_features:
            if feature not in self.cd_df.columns:
                raise ValueError(str(feature) + ' not in columns')

        self.cd_df = self.cd_df.drop(self.drop_features, axis=1)
