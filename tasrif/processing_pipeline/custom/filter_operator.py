"""
Operator to Filter rows or days from patients data
"""

import pandas as pd

from tasrif.processing_pipeline import ProcessingOperator


class FilterOperator(ProcessingOperator):
    """

    Examples
    --------

    >>> import pandas as pd
    >>> import numpy as np
    >>> import datetime
    >>>
    >>> from tasrif.processing_pipeline.custom import FilterOperator
    >>>
    >>> df = pd.DataFrame({
    ...         'Hours': pd.date_range('2018-01-01', '2018-01-10', freq='1H', closed='left'),
    ...         'Steps': np.random.randint(100,10000, size=9*24),
    ...         }
    ...      )
    >>>
    >>> ids = []
    >>> for i in range(1, 217):
    ...     ids.append(i%10 + 1)
    >>>
    >>> df["Id"] = ids
    >>>
    >>>
    >>> # Add day for id 1
    >>> df = df.append({'Hours': datetime.datetime(2020, 2, 2), 'Steps': 2000, 'Id': 1}, ignore_index=True)
    >>>
    >>> # Remove 5 days from id 10
    >>> id_10_indices = df.loc[df.Id == 10].index.values[:-5]
    >>> df = df[~df.index.isin(id_10_indices)]
    >>>
    >>> operator = FilterOperator(participant_identifier="Id",
    ...                           date_feature_name="Hours",
    ...                           epoch_filter=lambda df: df['Steps'] > 10,
    ...                           day_filter={
    ...                               "column": "Hours",
    ...                               "filter": lambda x: x.count() < 10,
    ...                               "consecutive_days": (7, 12) # 7 minimum consecutive days, and 12 max
    ...                           },
    ...                           filter_type="include")
    >>> operator.process(df)[0]
    Hours   Steps   Id
    0   2018-01-01 09:00:00     6232    1
    1   2018-01-01 19:00:00     4623    1
    2   2018-01-02 05:00:00     4094    1
    3   2018-01-02 15:00:00     1800    1
    4   2018-01-03 01:00:00     1861    1
    ...     ...     ...     ...
    190     2018-01-07 23:00:00     9116    9
    191     2018-01-08 09:00:00     7265    9
    192     2018-01-08 19:00:00     4608    9
    193     2018-01-09 05:00:00     8709    9
    194     2018-01-09 15:00:00     8970    9

    >>> df = pd.DataFrame([
    ...     [1, "2020-05-01 00:00:00", "1", "3"],
    ...     [1, "2020-05-01 01:00:00", "1", "5" ],
    ...     [2, "2020-05-01 03:00:00", "2", "3"],
    ...     [2, "2020-05-02 00:00:00", "1", "10"],
    ...     [3, "2020-05-02 01:00:00", "1", "0"],
    ...     [4, "2020-05-03 01:00:00", "1", "0"]],
    ...     columns=['logId', 'timestamp', 'sleep_level', 'awake_count'])
    >>>
    >>> op = FilterParticipantsOperator(participant_identifier="logId",
    ...                                 participants=[1, 3],
    ...                                 filter_type="include",)
    >>> df1 = op.process(df)
    >>> df1[0]
    logId   timestamp   sleep_level     awake_count
    0   1   2020-05-01 00:00:00     1   3
    1   1   2020-05-01 01:00:00     1   5
    4   3   2020-05-02 01:00:00     1   0

    """
    def __init__(  #pylint: disable=too-many-arguments
            self,
            participant_identifier="id",
            date_feature_name="time",
            epoch_filter=None,
            day_filter=None,
            participant_filter=None,
            filter_type="include"):
        """
        Initializes the operator

        Args:
            participant_identifier (str):
              patient identifier column
            date_feature_name (str):
                time series column
            epoch_filter (str, callable):
                row filter
            day_filter (str, callable):
                filter the days per patient
            participant_filter (list):
                participants to include or exclude
            filter_type (str):
                include the filtered epochs, and days, or exclude them.
        """
        super().__init__()
        self.participant_identifier = participant_identifier
        self.date_feature_name = date_feature_name
        self.epoch_filter = epoch_filter
        self.day_filter = day_filter
        self.participant_filter = participant_filter
        self.filter_type = filter_type

    def _process(self, *data_frames):
        """Process the passed data using the processing configuration specified
        in the constructor

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            data_frames
                Processed data frames
        """

        processed = []

        for data_frame in data_frames:
            processed_df = None
            processed_df = self._process_participants(data_frame)
            processed_df = self._process_epoch(processed_df)
            processed_df = self._process_day(processed_df)
            processed.append(processed_df)

        return processed

    def _process_epoch(self, data_frame):
        """ filters rows based on self.epoch_filter

        Args:
            data_frame (pd.DataFrame):
                pandas dataframes to be filtered by row.

        Returns:
            processed_df (pd.DataFrame):
                filtered pandas DataFrame

        """
        if not self.epoch_filter:
            return data_frame

        if isinstance(self.epoch_filter, str):
            if self.filter_type == "include":
                processed_df = data_frame.query(self.epoch_filter)
            else:
                processed_df = data_frame.query(f"not ({self.epoch_filter})")
        else:
            if self.filter_type == "include":
                processed_df = data_frame[self.epoch_filter(data_frame)]
            else:
                processed_df = data_frame[~self.epoch_filter(data_frame)]

        return processed_df

    def _process_day(self, processed_df):
        """ filters patient days based on self.day_filter

        Args:
            processed_df (pd.DataFrame):
                Pandas DataFrame that has been filtered by `_process_epoch`

        Returns:
            processed_df (pd.DataFrame):
                filtered dataframe by dat

        """
        if self.day_filter:

            if isinstance(self.day_filter, dict):
                index = processed_df.groupby([
                    self.participant_identifier,
                    processed_df[self.date_feature_name].dt.date
                ])[self.day_filter['column']].transform(
                    self.day_filter['filter'])

                if self.filter_type == "include":
                    processed_df = processed_df.loc[index]
                else:
                    processed_df = processed_df.loc[~index]

                if ('consecutive_days'
                        in self.day_filter) and not processed_df.empty:
                    processed_df = self._consecutive_days_filter(
                        processed_df, self.day_filter['consecutive_days'])

            elif type(self.day_filter == list):
                for day_filter_item in self.day_filter:
                    index = processed_df.groupby([
                        self.participant_identifier,
                        processed_df[self.date_feature_name].dt.date
                    ])[day_filter_item['column']].transform(
                        day_filter_item['filter'])

                    if self.filter_type == "include":
                        processed_df = processed_df.loc[index]
                    else:
                        processed_df = processed_df.loc[~index]

                    if ('consecutive_days'
                            in day_filter_item) and not processed_df.empty:
                        processed_df = self._consecutive_days_filter(
                            processed_df, day_filter_item['consecutive_days'])

        return processed_df

    def _consecutive_days_filter(self, dataframe, consecutive_days_range):
        """Helper function for `_process_day`. Finds the number of consecutive days
        per participant, and filters them out of the dataframe if the number is out of
        the range of `consecutive_days_range` (exclusive, inclusive)

        Args:
            dataframe (pd.DataFrame):
                Variable number of pandas dataframes to be processed
            consecutive_days_range (int, 2-tuple of ints):
                - *int*: minimum number of days (exclusive) for the consecutive days
                    to be included within the returned dataframe.
                - a 2-tuple of ints that represent the minimum (exclusive) number of consecutive days,
                    and the maximum number of consecutive days (inclusive) to be included within the
                    returned dataframe (see example).

        Returns:
            dataframe (pd.DataFrame):
                dataframe that include consecutive days as per the criteria.

        """

        days_per_id = dataframe.groupby([
            self.participant_identifier,
            pd.Grouper(key=self.date_feature_name, freq='D')
        ]).sum().reset_index()

        # Take the date difference between a row and its previous row
        # If the difference is not 1 day, we start a new label for the sequence using pd.cumsum()
        label_of_consecutive_days_per_id = days_per_id.groupby(
            self.participant_identifier)[
                self.date_feature_name].diff().dt.days.ne(1).cumsum()
        consecutive_days_per_id = days_per_id.groupby(
            [self.participant_identifier,
             label_of_consecutive_days_per_id]).size().reset_index(level=1,
                                                                   drop=True)

        # Match the index with the sequence labels
        consecutive_days_per_id = consecutive_days_per_id.reset_index()
        consecutive_days_per_id.index = consecutive_days_per_id.index + 1

        # Find the sequence label that matches the criteria
        if isinstance(consecutive_days_range, tuple):
            min_consecutive_days = consecutive_days_range[0]
            max_consecutive_days = consecutive_days_range[1]
            sequences_to_keep = consecutive_days_per_id[
                (consecutive_days_per_id[0] > min_consecutive_days)
                & (consecutive_days_per_id[0] <= max_consecutive_days
                   )].index.values
        else:
            sequences_to_keep = consecutive_days_per_id[
                consecutive_days_per_id[0] >
                consecutive_days_range].index.values

        # Find days to keep: Given the day sequence labels, include the one's in sequences_to_keep
        days_to_keep = days_per_id[label_of_consecutive_days_per_id.isin(
            sequences_to_keep)]

        dataframe = dataframe.groupby(self.participant_identifier).apply(
            lambda x: self._keep_days_in_df(x, days_to_keep)).reset_index(
                drop=True)
        return dataframe

    def _keep_days_in_df(self, dataframe, days_to_keep):
        """Helper function for consecutive_days_filter

        Args:
            dataframe (pd.DataFrame):
                dataframe of all days.
            days_to_keep (pd.DataFrame):
                dataframe of consecutive days that match the criteria

        Returns:
            dataframe (pd.DataFrame):
                dataframe that has been filtered using `days_to_keep`

        """

        days = days_to_keep.loc[days_to_keep[self.participant_identifier] ==
                                dataframe.name, self.date_feature_name].dt.date
        return dataframe[dataframe[self.date_feature_name].dt.date.isin(days)]

    def _process_participants(self, data_frame):
        """ filters rows based on self.epoch_filter

        Args:
            data_frame (pd.DataFrame):
                pandas dataframes to be filtered by row.

        Returns:
            processed_df (pd.DataFrame):
                filtered pandas DataFrame

        """
        if not self.participant_filter:
            return data_frame

        data_frame_filter = data_frame[self.participant_identifier].isin(
            self.participant_filter)
        if self.filter_type == "include":
            processed_df = data_frame[data_frame_filter]
        else:
            processed_df = data_frame[~data_frame_filter]

        return processed_df
