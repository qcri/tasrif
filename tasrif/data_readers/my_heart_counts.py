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

from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.custom import SetFeaturesValueOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator
from tasrif.processing_pipeline.custom import AggregateOperator
from tasrif.processing_pipeline.pandas import DropFeaturesOperator
from tasrif.processing_pipeline.pandas import DropDuplicatesOperator
from tasrif.processing_pipeline.pandas import DropNAOperator


class MyHeartCountsDataset:#pylint: disable=too-few-public-methods
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


        Default PIPELINE Pseudocode
        Modifies self.dcs_df by dropping unnecessary columns (features), 
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
            - average the intensities if self.merge_activity_features is true, set it to column 'activity_intensity'
            - average the time if self.merge_activity_features is true, set it to column 'activity_time'
            - return dcs_df

        """
        PIPELINE = ProcessingPipeline([DropFeaturesOperator(['appVersion', 'phoneInfo', 'activity1_type', 'activity2_type', 'phone_on_user']),
                                       CreateFeatureOperator(feature_name='activity1_option', feature_creator=lambda df: bool(df['activity1_option'])),
                                       CreateFeatureOperator(feature_name='activity2_option', feature_creator=lambda df: bool(df['activity2_option'])),
                                       SetFeaturesValueOperator(selector=lambda df: df['activity1_option'].isnull() & df['activity2_option'].notnull(), 
                                                                features=['activity1_option'], value=False),
                                       SetFeaturesValueOperator(selector=lambda df: df['activity2_option'].isnull() & df['activity1_option'].notnull(), 
                                                                features=['activity2_option'], value=False),
                                       SetFeaturesValueOperator(selector=lambda df: pd.notnull(df['activity1_option']) & df['activity1_option'] & \
                                                                                    pd.notnull(df['activity1_time']) & pd.isnull(df['activity1_intensity']), 
                                                                features=['activity1_intensity'], value=4),
                                       SetFeaturesValueOperator(selector=lambda df: pd.notnull(df['activity2_option']) & df['activity2_option'] & \
                                                                                    pd.notnull(df['activity2_time']) & pd.isnull(df['activity2_intensity']), 
                                                                features=['activity2_intensity'], value=4),
                                       SetFeaturesValueOperator(selector=lambda df: pd.notnull(df['activity1_option']) & ~df['activity1_option'], 
                                                                features=['activity1_intensity', 'activity1_time'], value=0),
                                       SetFeaturesValueOperator(selector=lambda df: pd.notnull(df['activity2_option']) & ~df['activity2_option'], 
                                                                features=['activity2_intensity', 'activity2_time'], value=0),
                                       DropFeaturesOperator(['activity1_option', 'activity2_option']),
                                       CreateFeatureOperator(feature_name='activity_intensity', 
                                                             feature_creator=lambda df: (df['activity1_intensity'] + df['activity2_intensity'])/2),
                                       CreateFeatureOperator(feature_name='activity_time',
                                                             feature_creator=lambda df: (df['activity1_time'] + df['activity2_time'])/2)
                                      ])

        """ Default GROUP_PIPELINE Pseudocode
        Modifies self.dcs_df by dropping unnecessary columns (features), 
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
        """
        GROUP_PIPELINE = ProcessingPipeline([DropFeaturesOperator(['recordId']),
                                             AggregateOperator(groupby_feature_names ="healthCode",
                                                               aggregation_definition={'activity1_intensity' : ['mean', 'std'],\
                                                                                       'activity1_time' : ['mean', 'std'],\
                                                                                       'activity2_intensity' : ['mean', 'std'],\
                                                                                       'activity2_time' : ['mean', 'std'],\
                                                                                       'sleep_time' : ['mean', 'std']})])



    def __init__(self,\
        mhc_folder='~/Documents/Data/MyHeartCounts',\
        dcs_filename='Daily Check Survey.csv',\
        processing_pipeline: ProcessingPipeline = Defaults.PIPELINE,\
        group_pipeline: ProcessingPipeline = Defaults.GROUP_PIPELINE
        ): #pylint: disable=too-many-arguments

        full_path = pathlib.Path(mhc_folder, dcs_filename)
        self.dcs_df = pd.read_csv(full_path)
        self.raw_df = self.dcs_df.copy()
        self.processing_pipeline = processing_pipeline
        self.group_pipeline = group_pipeline

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
        """
        Returns
        -------
        sets the result in self.dcs_df
        """

        self.dcs_df = self.processing_pipeline.process(self.dcs_df)[0]

    def _group(self):
        """
        Returns
        -------
        sets the result in self.group_dcs_df
        """
        self.group_dcs_df = self.group_pipeline.process(self.dcs_df)[0]

class DayOneSurveyDataset: #pylint: disable=too-many-arguments
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
        processing_pipeline: ProcessingPipeline = ProcessingPipeline([DropNAOperator(subset=['device', 'labwork'])])):

        full_path = pathlib.Path(mhc_folder, dos_filename)
        self.dos_df = pd.read_csv(full_path)
        self.raw_df = self.dos_df.copy()
        self.processing_pipeline = processing_pipeline

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

        self.dos_df = self.processing_pipeline.process(self.dos_df)[0]


class PARQSurveyDataset: #pylint: disable=too-many-arguments
    """Class to work with the PARQ Survey Table in the MyHeartCounts dataset.

    """

    parq_df = None
    raw_df = None

    def __init__(self,\
        mhc_folder='~/Documents/Data/MyHeartCounts',\
        dos_filename='PAR-Q Survey.csv',\
        processing_pipeline: ProcessingPipeline = ProcessingPipeline([DropNAOperator(subset=['chestPain', 'chestPainInLastMonth', 'dizziness', 
                                                                                            'heartCondition', 'jointProblem', 'physicallyCapable',
                                                                                            'prescriptionDrugs']),
                                                                     DropDuplicatesOperator(subset=['healthCode'], keep='last')])
                                                                    ):

        full_path = pathlib.Path(mhc_folder, dos_filename)
        self.parq_df = pd.read_csv(full_path)
        self.raw_df = self.parq_df.copy()
        self.processing_pipeline = processing_pipeline

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
        self.parq_df = self.processing_pipeline.process(self.parq_df)[0]

class RiskFactorSurvey:
    """Class to work with the Risk factor Survey Table in the MyHeartCounts dataset.

    """

    class Default:#pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        """
        DROP_FEATURES = []

    def __init__(self,\
        mhc_folder='~/Documents/Data/MyHeartCounts',\
        rfs_filename='Risk Factor Survey.csv',\
        processing_pipeline: ProcessingPipeline = ProcessingPipeline([DropFeaturesOperator(Default.DROP_FEATURES)])):

        full_path = pathlib.Path(mhc_folder, rfs_filename)
        self.rf_df = pd.read_csv(full_path)
        self.raw_df = self.rf_df.copy()
        self.processing_pipeline = processing_pipeline

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
        self.rf_df = self.processing_pipeline.process(self.rf_df)[0]

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
        processing_pipeline: ProcessingPipeline = ProcessingPipeline([DropFeaturesOperator(Default.DROP_FEATURES)])):

        full_path = pathlib.Path(mhc_folder, cds_filename)
        self.cd_df = pd.read_csv(full_path)
        self.raw_df = self.cd_df.copy()
        self.processing_pipeline = processing_pipeline

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
        self.cd_df = self.processing_pipeline.process(self.cd_df)[0]
