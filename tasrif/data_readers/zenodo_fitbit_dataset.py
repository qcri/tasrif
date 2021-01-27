"""Module that provides classes to work with the fitbit dataset on the Zenodo platform
collected by crowd sourcing.
"""

import pathlib
from datetime import datetime
import pandas as pd


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
        DROP_COLUMNS = ["TrackerDistance", 'LoggedActivitiesDistance', 'VeryActiveDistance', 'ModeratelyActiveDistance', 'SedentaryActiveDistance', "LightActiveDistance"]

    def __init__(self, zenodo_folder, drop_columns=Default.DROP_COLUMNS, accumulate_active_minutes=True):

        subfolder_1 = 'Fitabase Data 3.12.16-4.11.16'
        subfolder_2 = 'Fitabase Data 4.12.16-5.12.16'
        full_path_1 = pathlib.Path(zenodo_folder, subfolder_1)
        full_path_2 = pathlib.Path(zenodo_folder, subfolder_2)

        day_act1 = pathlib.Path(full_path_1, 'dailyActivity_merged.csv')
        day_act2 = pathlib.Path(full_path_2, 'dailyActivity_merged.csv')
        raw_df1 = pd.read_csv(day_act1)
        raw_df2 = pd.read_csv(day_act2)

        self.drop_columns = drop_columns
        self.accumulate_active_minutes = accumulate_active_minutes

        self.raw_df = pd.concat([raw_df1, raw_df2], axis=0, ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, copy=True)
        self.raw_df = self.raw_df.dropna()
        self.zadf = self.raw_df.copy()
        self._process()
        self._group()

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
        if self.drop_columns:
            self.zadf = self.zadf.drop(self.drop_columns, axis=1)
        if self.accumulate_active_minutes:
            self.zadf['ActiveMinutes'] = self.zadf.VeryActiveMinutes + self.zadf.FairlyActiveMinutes + self.zadf.LightlyActiveMinutes
            self.zadf = self.zadf.drop(['VeryActiveMinutes', "FairlyActiveMinutes", "LightlyActiveMinutes"], axis=1)

        self.zadf.rename(columns={'ActivityDate': 'Date'}, inplace=True)
        self.zadf['Date'] = pd.to_datetime(self.zadf['Date'])

    def _group(self):
        stats = ['mean', 'std']
        group_agg = {
            "TotalSteps" : stats,
            "TotalDistance" : stats,
            "SedentaryMinutes" : stats,
            "Calories" : stats,
            'ActiveMinutes' : stats
            }

        columns = ['Id']
        for key, value in group_agg.items():
            for i in value:
                columns.append(f'{key}_{i}')

        self.group_df = self.zadf.copy()
        self.group_df = self.group_df.groupby('Id', as_index=False).agg(group_agg)
        self.group_df.columns = columns

class ZenodoFitbitWeightDataset:
    """Class that represents the body weight related CSV files of the fitbit dataset published on Zenodo
    """

    class Default:#pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        """
        DROP_COLUMNS = ['Fat', 'WeightPounds', 'IsManualReport']


    def __init__(self, zenodo_folder, drop_columns=Default.DROP_COLUMNS):

        subfolder_1 = 'Fitabase Data 3.12.16-4.11.16'
        subfolder_2 = 'Fitabase Data 4.12.16-5.12.16'
        full_path_1 = pathlib.Path(zenodo_folder, subfolder_1)
        full_path_2 = pathlib.Path(zenodo_folder, subfolder_2)

        day_wt1 = pathlib.Path(full_path_1, 'weightLogInfo_merged.csv')
        day_wt2 = pathlib.Path(full_path_2, 'weightLogInfo_merged.csv')
        raw_df1 = pd.read_csv(day_wt1)
        raw_df2 = pd.read_csv(day_wt2)

        self.drop_columns = drop_columns

        self.raw_df = pd.concat([raw_df1, raw_df2], axis=0, ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, copy=True)
        self.zwdf = self.raw_df.copy()
        self._process()
        self._group()

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
        if self.drop_columns:
            self.zwdf = self.zwdf.drop(self.drop_columns, axis=1)

    def _group(self):
        stats = ['mean', 'std']
        group_agg = {
            "WeightKg" : stats,
            "BMI" : stats,
            }

        columns = ['Id']
        for key, value in group_agg.items():
            for i in value:
                columns.append(f'{key}_{i}')

        self.group_df = self.zwdf.copy()
        self.group_df = self.group_df.groupby('Id', as_index=False).agg(group_agg)
        self.group_df.columns = columns

class ZenodoFitbitSleepDataset:
    """Class that represents the sleep related CSV files of the fitbit dataset published on Zenodo
    """

    def __init__(self, zenodo_folder):

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
        self._process()
        self._group()

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
        self.zsdf.drop_duplicates(subset=['Id', 'logId', 'date'], keep='first', inplace=True)
        self.zsdf[' edate'] = pd.to_datetime(self.zsdf['date'])
        self.zsdf['duration'] = self.zsdf['date'].sub(self.zsdf['date'].shift())
        now = datetime.now()
        zero_duration = now - now
        # Change the duration of the first entry of every sleep log  group to zero
        self.zsdf.loc[self.zsdf.groupby('logId')['duration'].head(1).index, 'duration'] = zero_duration
        self.zsdf = self.zsdf.groupby(['logId', 'Id'], as_index=False).agg({'duration': ['sum'], 'date': ['first'], 'value': ['mean']})
        self.zsdf.columns = ['logId', 'Id', 'total_sleep', 'date', 'sleep_level']
        self.zsdf['date'] = self.zsdf['date'].dt.date


    def _group(self):
        self.group_df = self.zsdf.copy()
        self.group_df['total_sleep_secs'] = self.group_df.total_sleep.dt.total_seconds()
        self.group_df = self.group_df.groupby('Id', as_index=False).agg({
            'logId': ['count'],
            'total_sleep_secs': ['mean', 'std'],
            'sleep_level': ['mean', 'std']})
        self.group_df.columns = [
            'Id', 'sleep_episodes_count',
            'total_sleep_mean', 'total_sleep_std',
            'sleep_level_mean', 'sleep_level_std']
