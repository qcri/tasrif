"""Module that provides classes to work with the fitbit dataset on the Zenodo platform
collected by crowd sourcing.
"""

import pathlib
import pandas as pd

from tasrif.processing_pipeline import  (
    ProcessingPipeline,
    SequenceOperator,
    ComposeOperator,
    NoopOperator)
from tasrif.processing_pipeline.custom import (
    CreateFeatureOperator,
    AggregateOperator,
    AddDurationOperator)
from tasrif.processing_pipeline.pandas import (
    DropNAOperator,
    DropFeaturesOperator,
    DropDuplicatesOperator)

class ZenodoCompositeFitbitDataset:
    """Class to work with multiple zenodo datasets by merging/concatenating the features.
    """
    def __init__(self, zenodo_datasets):
        self.zenodo_datasets = zenodo_datasets
        self._process()

    def grouped_dataframe(self):
        """Gets the dataframe grouped by participants. The result is a data frames where each row
        represents exactly one partipant

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.group_df

    def _process(self):
        dfs = []
        for zds in self.zenodo_datasets:
            dfs.append(zds.grouped_dataframe())

        self.group_df = pd.concat(dfs, axis=1, join='inner')


class ZenodoFitbitActivityDataset:
    """Class that represents the activity related CSV files of the fitbit dataset published on Zenodo
    """

    class Default:#pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        """
        DROP_FEATURES = [
            "TrackerDistance",
            'LoggedActivitiesDistance',
            'VeryActiveDistance',
            'ModeratelyActiveDistance',
            'SedentaryActiveDistance',
            "LightActiveDistance",
            "ActivityDate"]

        AGGREGATION_FUNCS = ['mean', 'std']
        AGGREGATION_DEFINITION = {
            "TotalSteps" : AGGREGATION_FUNCS,
            "TotalDistance" : AGGREGATION_FUNCS,
            "SedentaryMinutes" : AGGREGATION_FUNCS,
            "Calories" : AGGREGATION_FUNCS,
            'ActiveMinutes' : AGGREGATION_FUNCS
            }

        PIPELINE = SequenceOperator([
            DropNAOperator(),
            CreateFeatureOperator(
                feature_name="ActiveMinutes",
                feature_creator=lambda df: df['VeryActiveMinutes'] + df["FairlyActiveMinutes"] + df["LightlyActiveMinutes"]),
            CreateFeatureOperator(
                feature_name="Date", feature_creator=lambda df: pd.to_datetime(df['ActivityDate'])),
            DropFeaturesOperator(drop_features=DROP_FEATURES),
            ComposeOperator([
                NoopOperator(),
                AggregateOperator(groupby_feature_names="Id", aggregation_definition=AGGREGATION_DEFINITION)
            ])])

    def __init__(self, zenodo_folder, pipeline=Default.PIPELINE):

        subfolder_1 = 'Fitabase Data 3.12.16-4.11.16'
        subfolder_2 = 'Fitabase Data 4.12.16-5.12.16'
        full_path_1 = pathlib.Path(zenodo_folder, subfolder_1)
        full_path_2 = pathlib.Path(zenodo_folder, subfolder_2)

        day_act1 = pathlib.Path(full_path_1, 'dailyActivity_merged.csv')
        day_act2 = pathlib.Path(full_path_2, 'dailyActivity_merged.csv')
        raw_df1 = pd.read_csv(day_act1)
        raw_df2 = pd.read_csv(day_act2)

        self.raw_df = pd.concat([raw_df1, raw_df2], axis=0, ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, copy=True)

        self.pipeline = pipeline

        self._process()

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
        return self.zadf

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """

        return self.zadf['Id'].nunique()

    def grouped_dataframe(self):
        """Gets the dataframe grouped by participants. The result is a data frames where each row
        represents exactly one partipant

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.group_df

    def participant_dataframes(self):
        """Gets and array of individual participant data frames

        Returns
        -------
        pd.Dataframe[]
            Array of dataframe objects representing the data from each partipant
        """

        return [pd.DataFrame(y) for x, y in self.zadf.groupby('Id', as_index=False)]

    def _process(self):
        zadf, group_df = (self.pipeline.process(self.raw_df))
        self.zadf, self.group_df = zadf[0], group_df[0]

class ZenodoFitbitWeightDataset:
    """Class that represents the body weight related CSV files of the fitbit dataset published on Zenodo
    """

    class Default:#pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        """
        DROP_COLUMNS = ['Fat', 'WeightPounds', 'IsManualReport']

        AGGREGATION_FUNCS = ['mean', 'std']
        AGGREGATION_DEFINITION = {
            "WeightKg" : AGGREGATION_FUNCS,
            "BMI" : AGGREGATION_FUNCS,
            }

        PIPELINE = SequenceOperator([
            DropFeaturesOperator(drop_features=DROP_COLUMNS),
            ComposeOperator([
                NoopOperator(),
                AggregateOperator(groupby_feature_names="Id", aggregation_definition=AGGREGATION_DEFINITION)
            ])
        ])


    def __init__(self, zenodo_folder, pipeline=Default.PIPELINE):

        self.pipeline = pipeline
        subfolder_1 = 'Fitabase Data 3.12.16-4.11.16'
        subfolder_2 = 'Fitabase Data 4.12.16-5.12.16'
        full_path_1 = pathlib.Path(zenodo_folder, subfolder_1)
        full_path_2 = pathlib.Path(zenodo_folder, subfolder_2)

        day_wt1 = pathlib.Path(full_path_1, 'weightLogInfo_merged.csv')
        day_wt2 = pathlib.Path(full_path_2, 'weightLogInfo_merged.csv')
        raw_df1 = pd.read_csv(day_wt1)
        raw_df2 = pd.read_csv(day_wt2)

        self.raw_df = pd.concat([raw_df1, raw_df2], axis=0, ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, copy=True)
        self.zwdf = self.raw_df.copy()
        self._process()

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
        return self.zwdf

    def participant_dataframes(self):
        """Gets and array of individual participant data frames

        Returns
        -------
        pd.Dataframe[]
            Array of dataframe objects representing the data from each partipant
        """
        return [pd.DataFrame(y) for x, y in self.zwdf.groupby('Id', as_index=False)]

    def grouped_dataframe(self):
        """Gets the dataframe grouped by participants. The result is a data frames where each row
        represents exactly one partipant

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.group_df

    def _process(self):
        zwdf, group_df = (self.pipeline.process(self.raw_df))
        self.zwdf, self.group_df = zwdf[0], group_df[0]


class ZenodoFitbitSleepDataset:
    """Class that represents the sleep related CSV files of the fitbit dataset published on Zenodo
    """

    class Default:

        DAILY_AGGREGATION_DEFINITION = {'duration': ['sum'], 'date': ['first'], 'value': ['mean']}

        TOTAL_AGGREGATION_DEFINITION = {
            'logId': ['count'],
            'total_sleep_seconds': ['mean', 'std'],
            'value_mean': ['mean', 'std']}

        PIPELINE = SequenceOperator([
            #DropDuplicatesOperator(subset=['Id', 'logId', 'date'], keep='first', inplace=True),
            CreateFeatureOperator(
                feature_name="date",
                feature_creator=lambda df: pd.to_datetime(df['date'])),
            AddDurationOperator(groupby_feature_names="logId", timestamp_feature_name="date"),
            AggregateOperator(
                groupby_feature_names=['logId', 'Id'],
                aggregation_definition=DAILY_AGGREGATION_DEFINITION
            ),
            ComposeOperator([
                NoopOperator(),
                ComposeOperator([
                    CreateFeatureOperator(
                        feature_name="total_sleep_seconds",
                        feature_creator=lambda df: df.duration_sum.total_seconds()
                    ),
                    AggregateOperator(
                        groupby_feature_names="Id",
                        aggregation_definition=TOTAL_AGGREGATION_DEFINITION
                    )]
                )
            ])
        ])

    def __init__(self, zenodo_folder, pipeline=Default.PIPELINE):

        subfolder_1 = 'Fitabase Data 3.12.16-4.11.16'
        subfolder_2 = 'Fitabase Data 4.12.16-5.12.16'
        full_path_1 = pathlib.Path(zenodo_folder, subfolder_1)
        full_path_2 = pathlib.Path(zenodo_folder, subfolder_2)

        day_s1 = pathlib.Path(full_path_1, 'minuteSleep_merged.csv')
        day_s2 = pathlib.Path(full_path_2, 'minuteSleep_merged.csv')
        raw_df1 = pd.read_csv(day_s1)
        raw_df2 = pd.read_csv(day_s2)

        self.raw_df = pd.concat([raw_df1, raw_df2], axis=0, ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, copy=True)
        self.zsdf = self.raw_df.copy()
        self.pipeline = pipeline
        self._process()

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
        return self.zsdf

    def participant_dataframes(self):
        """Gets and array of individual participant data frames

        Returns
        -------
        pd.Dataframe[]
            Array of dataframe objects representing the data from each partipant
        """
        return [pd.DataFrame(y) for x, y in self.zsdf.groupby('Id', as_index=False)]

    def grouped_dataframe(self):
        """Gets and array of individual participant data frames

        Returns
        -------
        pd.Dataframe[]
            Array of dataframe objects representing the data from each partipant
        """
        return self.group_df

    def _process(self):
        zsdf, group_df = self.pipeline.process(self.raw_df)
        self.zsdf, self.group_df = zsdf[0], group_df[1][0]
