"""
Operator to merge gaps between activity.
"""

import pandas as pd

from tasrif.processing_pipeline import ProcessingOperator


class MergeFragmentedActivityOperator(ProcessingOperator):

    """
    Operator to merge gaps between activity. A gap is a time period of activity less than a given threshold.
    A period of activity that is larger than threshold is considered the end of an activity.
    Gaps found before the end of the activity will be merged with the activity.
    The end date of the activity will be set to the merged `end_date_feature_name`
    The input of this operator needs to have two datetime columns that represent the starting time of the
    activity and end time of the activity, respectively.


    >>> import pandas as pd
    >>> import numpy as np
    >>> from tasrif.processing_pipeline.custom import MergeFragmentedActivityOperator
    >>> from tasrif.processing_pipeline.custom import CreateFeatureOperator
    >>> df = pd.DataFrame([
    ...    [0,2,354,27,5,386,0.91,'2016-03-27 03:33:00', '2016-03-27 09:02:00'],
    ...    [0,4,312,23,7,321,0.93,'2016-03-28 00:40:00', '2016-03-28 01:56:00'],
    ...    [0,5,312,35,7,193,0.93,'2016-03-29 00:40:00', '2016-03-29 01:56:00'],
    ...    [0,7,312,52,7,200,0.93,'2016-05-21 00:40:00', '2016-05-21 01:56:00'],
    ...    [0,8,312,12,7,43,0.93,'2016-05-23 01:57:00', '2016-05-23 01:58:00'],
    ...    [0,9,312,42,7,100,0.93,'2016-05-23 01:59:00', '2016-05-23 01:59:30'],
    ...    [0,9,312,21,7,302,0.93,'2016-05-23 03:00:00', '2016-05-23 03:59:30'],
    ...    [0,10,312,16,7,335,0.93,'2016-05-23 10:57:00', '2016-05-23 20:58:00'],
    ...    [0,11,312,16,7,335,0.93,'2016-10-24 00:58:00', '2016-05-24 01:58:00'],
    ...    [1,3,312,16,7,335,0.93,"2016-03-14 08:12:00","2016-03-14 10:15:00"],
    ...    [1,4,272,26,5,303,0.89,"2016-03-16 03:12:00","2016-03-16 08:14:00"],
    ...    [1,5,61,2,0,63,0.96,"2016-03-16 19:43:00","2016-03-16 20:45:00"],
    ...    [1,6,402,34,1,437,0.91,"2016-03-17 01:16:00","2016-03-17 08:32:00"],
    ... ],
    ...     columns=["Id",
    ...           "logId",
    ...           "Total Minutes Asleep",
    ...           "Total Minutes Restless",
    ...           "Total Minutes Awake",
    ...           "Total Minutes in Bed",
    ...           "Sleep Efficiency",
    ...           "Sleep Start",
    ...           "Sleep End"])
    >>>
    >>> df['Sleep Start'] = pd.to_datetime(df['Sleep Start'])
    >>> df['Sleep End'] = pd.to_datetime(df['Sleep End'])
    >>> aggregation_definition = {
    ...    'logId': lambda df: df.iloc[0],
    ...    'Total Minutes Asleep': np.sum,
    ...    'Total Minutes Restless': np.sum,
    ...    'Total Minutes Awake': np.sum,
    ...    'Total Minutes in Bed': np.sum,
    ... }
    >>>
    >>> operator = MergeFragmentedActivityOperator(
    ...                                participant_identifier='Id',
    ...                                start_date_feature_name='Sleep Start',
    ...                                end_date_feature_name='Sleep End',
    ...                                threshold="3 hour",
    ...                                aggregation_definition=aggregation_definition)
    >>> df = operator.process(df)[0]
    >>> df
    Id  logId   Total Minutes Asleep    ...
    0   0   2   354     27  5   386     0.91    2016-03-27 03:33:00     2016-03-27 09:02:00
    1   0   4   312     23  7   321     0.93    2016-03-28 00:40:00     2016-03-28 01:56:00
    2   0   5   312     35  7   193     0.93    2016-03-29 00:40:00     2016-03-29 01:56:00
    3   0   7   312     52  7   200     0.93    2016-05-21 00:40:00     2016-05-21 01:56:00
    4   0   8   936     75  21  445     0.93    2016-05-23 01:57:00     2016-05-23 03:59:30
    7   0   10  312     16  7   335     0.93    2016-05-23 10:57:00     2016-05-23 20:58:00
    ...
    """
    def __init__( # pylint: disable=R0913
            self,
            participant_identifier,
            start_date_feature_name,
            end_date_feature_name,
            threshold="1 hour",
            aggregation_definition=None,
            return_before_merging=False):
        """Creates a new instance of MergeFragmentedActivityOperator

        Args:
            participant_identifier : str or list of str
                Name of the feature(s) identifying the participant
            start_date_feature_name : str
                Name of the feature to identify start of activity timestamp series
            end_date_feature_name : str
                Name of the feature to identify end of activity timestamp series
            threshold : str
                Time string passed to Pandas Timedelta function
            aggregation_definition : dict
                dict of column names as dict keys and callables as values.
                dict values can be a list of function to be applied on a single column.
                Used to downsample required columns if needed.
            return_before_merging : bool
                Whether to return the assigned groups and timedeltas before merging the dataframe.

        """
        super().__init__()

        threshold = pd.Timedelta(threshold)

        self.threshold = threshold
        self.participant_identifier = participant_identifier
        self.aggregation_definition = aggregation_definition
        self.start_date_feature_name = start_date_feature_name
        self.end_date_feature_name = end_date_feature_name
        self.return_before_merging = return_before_merging

    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            pd.DataFrame -or- list[pd.DataFrame]
                Processed dataframe(s) resulting from applying the operator
        """

        processed = []
        for data_frame in data_frames:
            data_frame = self._merge_frames(data_frame)
            processed.append(data_frame)

        return processed

    def _merge_frames(self, dataframe):
        ''' Pseudocode
        - add column group with 0s as value
        - assign group=1 to a row if 'timedelta' is greater than threshold
        -- 'timedelta' column is created by `_calculate_time_delta`
        - gaps are 0 encountered after a series of 1s
        - gaps is a 0 that becomes 1 when shifted
        - main_acivity is 1 encountered after a series of 0
        - main_acivity is 1 that becomes 0 when shifted

        Args:
            dataframe (pd.DataFrame):
              pandas dataframes to be processed

        Returns:
            output_dataframe (pd.DataFrame)
                merged dataframe
        '''
        output_dataframe = dataframe.copy()
        output_dataframe = output_dataframe.groupby(
            self.participant_identifier
        ).apply(self._calculate_time_delta)

        output_dataframe['group'] = 0
        output_dataframe = output_dataframe.groupby(self.participant_identifier).apply(self._assign_groups)
        if self.return_before_merging:
            return output_dataframe

        gaps = output_dataframe[
            (output_dataframe['group'] == 0)
            & (output_dataframe['group'].shift(1).fillna(1) == 1)].index

        main_activity = output_dataframe[(output_dataframe['group'] == 1) & (
            output_dataframe['group'].shift(1).fillna(1) == 0)].index

        for start_index, end_index in zip(gaps, main_activity):

            if self.aggregation_definition:
                output_dataframe = self._aggregate_columns(output_dataframe, start_index, end_index)

            output_dataframe.loc[start_index, self.end_date_feature_name] = \
                dataframe.loc[end_index, self.end_date_feature_name]

            output_dataframe = output_dataframe.loc[
                ~output_dataframe.index.
                isin(list(range(start_index + 1, end_index + 1)))]

        output_dataframe.drop(columns=['timedelta', 'group'], inplace=True)
        return output_dataframe


    def _assign_groups(self, dataframe):
        indexes = dataframe[(dataframe['timedelta'] > self.threshold)].index
        dataframe.loc[indexes, 'group'] = 1
        return dataframe

    def _calculate_time_delta(self, dataframe):
        '''timedelta is the time taken to start the next activity
        pulls next row\'s starting date and takes the difference with the current row\'s end date

        Args:
            dataframe (pd.DataFrame):
              pandas dataframes to be processed

        Returns:
            dataframe (pd.DataFrame)
                dataframe with `timedelta` column added
        '''
        dataframe['timedelta'] = \
            (dataframe[self.start_date_feature_name].shift(-1) - dataframe[self.end_date_feature_name]).fillna(
            pd.Timedelta("99 days"))
        return dataframe

    def _aggregate_columns(self, dataframe, start_index, end_index):
        for key, value in self.aggregation_definition.items():
            if isinstance(value, list):
                for idx, function in enumerate(value):
                    dataframe.loc[start_index, key + '_' + str(idx)] = function(
                        dataframe.loc[start_index:end_index, key])
            else:
                dataframe.loc[start_index, key] = value(
                    dataframe.loc[start_index:end_index, key])

        return dataframe
