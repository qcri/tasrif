"""
Operator to fill gaps between activity.
"""

import pandas as pd

from tasrif.processing_pipeline import ProcessingOperator


class MergeGapsBetweenActivityOperator(ProcessingOperator):
    """
    Operator to fill gaps between activity. A gap is a time period of activity less than a given threshold.
    The gap is merged with the previous activity preiod that is larger than the threshold.
    The input of this operator needs to have two datetime columns that represent the starting time of the
    activity and end time of the activity, respectively.


    >>> import pandas as pd
    >>> import numpy as np
    >>> from tasrif.processing_pipeline.custom import MergeGapsBetweenActivityOperator
    >>> from tasrif.processing_pipeline.custom import CreateFeatureOperator
    >>> df = pd.DataFrame([
    ...    [0,1,411,15,0,426,0.96,'2016-03-26 23:45:00', '2016-03-27 00:46:00'],
    ...    [0,20,354,27,5,386,0.91,'2016-03-27 01:15:00', '2016-03-27 03:23:00'],
    ...    [0,2,354,27,5,386,0.91,'2016-03-27 03:33:00', '2016-03-27 09:02:00'],
    ...    [0,3,312,16,7,335,0.93,'2016-03-28 00:40:00', '2016-03-28 01:56:00'],
    ...    [1,3,312,16,7,335,0.93,"2016-03-14 08:12:00","2016-03-14 10:15:00"],
    ...    [1,4,272,26,5,303,0.89,"2016-03-16 03:12:00","2016-03-16 08:14:00"],
    ...    [1,5,61,2,0,63,0.96,"2016-03-16 19:43:00","2016-03-16 20:45:00"],
    ...    [1,6,402,34,1,437,0.91,"2016-03-17 01:16:00","2016-03-17 08:32:00"],
    ...    [1,7,379,26,6,411,0.92,"2016-03-18 01:36:00","2016-03-18 08:26:00"],
    ...    [1,8,447,20,1,468,0.95,"2016-03-19 00:08:00","2016-03-19 07:55:00"],
    ...    [1,9,469,7,0,476,0.985,"2016-03-20 01:08:00","2016-03-20 09:03:00"],
    ...    [1,10,390,30,7,427,0.9,"2016-03-21 01:08:00","2016-03-21 08:14:00"],
    ...    [1,11,281,13,3,297,0.9,"2016-03-23 02:33:00","2016-03-23 07:29:00"],
    ...    [1,12,303,37,4,344,0.8,"2016-03-24 02:33:00","2016-03-24 08:16:00"],
    ...    [2,1,839,101,21,961,0.87,"2016-03-27 15:45:00","2016-03-28 07:45:00"],
    ...    [2,2,594,279,88,961,0.61,"2016-03-29 21:59:00","2016-03-30 13:59:00"],
    ...    [2,3,119,8,0,127,0.93,"2016-04-29 18:33:00","2016-04-29 20:39:00"],
    ...    [2,4,124,15,3,142,0.87,"2016-04-30 14:54:00","2016-04-30 17:15:00"],
    ...    [2,5,796,36,129,961,0.82,"2016-05-01 18:01:00","2016-05-02 10:01:00"],
    ...    [2,6,137,14,3,154,0.88,"2016-05-08 18:12:00","2016-05-08 20:45:00"],
    ...    [3,7,940,0,21,961,0.97,"2016-04-03 20:50:00","2016-04-04 12:50:00"],
    ...    [3,8,498,344,119,961,0.51,"2016-04-09 13:24:00","2016-04-10 05:24:00"],
    ...    [3,9,644,229,88,961,0.67,"2016-04-14 20:16:00","2016-04-15 12:16:00"],
    ...    [3,10,722,141,98,961,0.75,"2016-04-29 22:09:00","2016-04-30 14:09:00"],
    ...    [3,11,590,254,117,961,0.61,"2016-04-30 20:52:00","2016-05-01 12:52:00"],
    ...    [4,1,960,28,3,991,0.96,"2016-03-11 23:24:00","2016-03-13 05:26:00"],
    ...    [4,2,162,4,3,169,0.95,"2016-03-13 10:19:00","2016-03-13 13:07:00"],
    ...    [4,3,84,3,1,88,0.95,"2016-03-13 23:40:00","2016-03-14 01:07:00"],
    ...    [4,4,109,2,0,111,0.98,"2016-03-14 05:58:00","2016-03-14 07:48:00"],
    ...    [4,5,68,0,0,68,1.0,"2016-03-15 02:24:00","2016-03-15 03:31:00"],
    ...    [4,6,60,3,0,63,0.952,"2016-03-25 07:56:00","2016-03-25 08:58:00"],
    ...    [5,1,135,9,0,144,0.93,"2016-03-13 20:49:00","2016-03-13 23:12:00"],
    ...    [6,1,509,33,2,544,0.935,"2016-03-11 21:31:00","2016-03-12 06:34:00"],
    ...    [6,2,610,24,6,640,0.95,"2016-03-12 21:03:00","2016-03-13 07:42:00"],
    ...    [6,3,518,17,2,537,0.96,"2016-03-13 20:32:00","2016-03-14 05:28:00"]],
    ...    columns=["Id",
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
    >>> operator = MergeGapsBetweenActivityOperator(
    ...                                participant_identifier='Id',
    ...                                start_date_feature_name='Sleep Start',
    ...                                end_date_feature_name='Sleep End',
    ...                                threshold="3 hour",
    ...                                aggregation_definition=aggregation_definition)
    >>> df = operator.process(df)[0]
    >>> df
    Id  logId   Total Minutes Asleep    Total Minutes Restless  Total Minutes Awake ...
    0   0   1   411     15  0   426     0.960   2016-03-26 23:45:00     2016-03-27 00:46:00
    1   0   20  354     27  5   386     0.910   2016-03-27 01:15:00     2016-03-27 03:23:00
    2   0   2   354     27  5   386     0.910   2016-03-27 03:33:00     2016-03-27 09:02:00
    3   0   3   312     16  7   335     0.930   2016-03-28 00:40:00     2016-03-28 01:56:00
    4   1   3   312     16  7   335     0.930   2016-03-14 08:12:00     2016-03-14 10:15:00
    5   1   4   272     26  5   303     0.890   2016-03-16 03:12:00     2016-03-16 08:14:00
    6   1   5   61  2   0   63  0.960   2016-03-16 19:43:00     2016-03-16 20:45:00
    ...
    """
    def __init__( # pylint: disable=R0913
            self,
            participant_identifier,
            start_date_feature_name,
            end_date_feature_name,
            threshold="1 hour",
            aggregation_definition=None):
        """Creates a new instance of MergeGapsBetweenActivityOperator

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

        """
        super().__init__()

        threshold = pd.Timedelta(threshold)

        self.threshold = threshold
        self.participant_identifier = participant_identifier
        self.aggregation_definition = aggregation_definition
        self.start_date_feature_name = start_date_feature_name
        self.end_date_feature_name = end_date_feature_name

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
        output_dataframe = dataframe.copy()
        output_dataframe = output_dataframe.groupby(
            self.participant_identifier
        ).apply(self._calculate_time_delta)

        output_dataframe['group'] = 0
        output_dataframe = output_dataframe.groupby(self.participant_identifier).apply(self._assign_groups)
        activity_start = output_dataframe[
            (output_dataframe['group'] == 0)
            & (output_dataframe['group'].shift(1).fillna(0) == 1)].index

        activity_end = output_dataframe[(output_dataframe['group'] == 1) & (
            output_dataframe['group'].shift(1).fillna(1) == 0)].index

        for start_index, end_index in zip(activity_start, activity_end):

            if self.aggregation_definition:
                output_dataframe = self._aggregate_columns(output_dataframe, start_index, end_index)

            output_dataframe.loc[start_index, self.participant_identifier] = \
                output_dataframe.loc[start_index, self.participant_identifier]

            output_dataframe.loc[start_index, self.start_date_feature_name] = \
                output_dataframe.loc[start_index, self.start_date_feature_name]

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
        dataframe['group'] = dataframe['group'].cumsum()
        dataframe['group'] = dataframe['group'].diff().fillna(method='bfill')

        return dataframe

    def _calculate_time_delta(self, dataframe):
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
