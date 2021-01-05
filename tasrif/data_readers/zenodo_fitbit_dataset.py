import pathlib
from datetime import datetime

import pandas as pd


class ZenodoCompositeFitbitDataset:

    def __init__(self, zenodo_datasets):
        self.zenodo_datasets = zenodo_datasets
        self._process()

    def grouped_dataframe(self):
        return self.group_df

    def _process(self):
        dfs = []
        for zds in self.zenodo_datasets:
            dfs.append(zds.grouped_dataframe())

        self.group_df = pd.concat(dfs, axis=1, join='inner')


class ZenodoFitbitActivityDataset:

    raw_df = None
    df = None
    group_df = None

    def __init__(self, zenodo_folder, drop_columns = ["TrackerDistance", 'LoggedActivitiesDistance', 'VeryActiveDistance', 'ModeratelyActiveDistance', 'SedentaryActiveDistance', "LightActiveDistance"], accumulate_active_minutes = True):

        subfolder_1 = 'Fitabase Data 3.12.16-4.11.16'
        subfolder_2 = 'Fitabase Data 4.12.16-5.12.16'
        full_path_1 = pathlib.Path(zenodo_folder,  subfolder_1)
        full_path_2 = pathlib.Path(zenodo_folder, subfolder_2)

        day_act1 = pathlib.Path(full_path_1, 'dailyActivity_merged.csv')
        day_act2 = pathlib.Path(full_path_2, 'dailyActivity_merged.csv')
        raw_df1 = pd.read_csv(day_act1)
        raw_df2 = pd.read_csv(day_act2)

        self.drop_columns = drop_columns
        self.accumulate_active_minutes = accumulate_active_minutes

        self.raw_df = pd.concat([raw_df1, raw_df2], axis=0, ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, copy=True)
        self.raw_df = self.raw_df.dropna()
        self.df = self.raw_df.copy()
        self._process()
        self._group()

    def raw_dataframe(self):
        return self.raw_df

    def processed_dataframe(self):
        return self.df

    def participant_count(self):
        return self.df['Id'].nunique()

    def grouped_dataframe(self):
        return self.group_df

    def participant_dataframes(self):
        return [pd.DataFrame(y) for x, y in self.df.groupby('Id', as_index=False)]

    def _process(self):
        if self.drop_columns:
            self.df = self.df.drop(self.drop_columns, axis=1)
        if self.accumulate_active_minutes:
            self.df['ActiveMinutes'] = self.df.VeryActiveMinutes + self.df.FairlyActiveMinutes + self.df.LightlyActiveMinutes
            self.df = self.df.drop(['VeryActiveMinutes', "FairlyActiveMinutes", "LightlyActiveMinutes"], axis=1)

        self.df.rename(columns={'ActivityDate': 'Date'}, inplace=True)
        self.df['Date']= pd.to_datetime(self.df['Date'])

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

        self.group_df = self.df.copy()
        self.group_df = self.group_df.groupby('Id', as_index=False).agg(group_agg)
        self.group_df.columns = columns

class ZenodoFitbitWeightDataset:

    def __init__(self, zenodo_folder,  drop_columns = ['Fat', 'WeightPounds', 'IsManualReport']):

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
        self.df = self.raw_df.copy()
        self._process()
        self._group()

    def raw_dataframe(self):
        return self.raw_df

    def processed_dataframe(self):
        return self.df

    def participant_dataframes(self):
          return [pd.DataFrame(y) for x, y in self.df.groupby('Id', as_index=False)]

    def grouped_dataframe(self):
        return self.group_df

    def _process(self):
        if self.drop_columns:
            self.df = self.df.drop(self.drop_columns, axis=1)

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

        self.group_df = self.df.copy()
        self.group_df = self.group_df.groupby('Id', as_index=False).agg(group_agg)
        self.group_df.columns = columns

class ZenodoFitbitSleepDataset:

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
        self.df = self.raw_df.copy()
        self._process()
        self._group()

    def raw_dataframe(self):
        return self.raw_df

    def processed_dataframe(self):
        return self.df

    def participant_dataframes(self):
          return [pd.DataFrame(y) for x, y in self.df.groupby('Id', as_index=False)]

    def grouped_dataframe(self):
        return self.group_df

    def _process(self):
        self.df.drop_duplicates(subset=['Id', 'logId', 'date'], keep='first', inplace=True)
        self.df['date']= pd.to_datetime(self.df['date'])
        self.df['duration'] = self.df['date'].sub(self.df['date'].shift())
        now = datetime.now()
        zero_duration = now - now
        # Change the duration of the first entry of every sleep log  group to zero
        self.df.loc[self.df.groupby('logId')['duration'].head(1).index, 'duration'] = zero_duration
        self.df = self.df.groupby(['logId', 'Id'], as_index=False).agg({'duration': ['sum'], 'date': ['first'], 'value': ['mean']})
        self.df.columns = ['logId', 'Id', 'total_sleep', 'date', 'sleep_level']
        self.df['date'] = self.df['date'].dt.date


    def _group(self):
        self.group_df = self.df.copy()
        self.group_df['total_sleep_secs'] = self.group_df.total_sleep.dt.total_seconds()
        self.group_df = self.group_df.groupby('Id', as_index=False).agg({
            'logId': ['count'],
            'total_sleep_secs': ['mean', 'std'],
            'sleep_level': ['mean', 'std'] })
        self.group_df.columns = [
            'Id', 'sleep_episodes_count',
            'total_sleep_mean', 'total_sleep_std',
            'sleep_level_mean', 'sleep_level_std']
