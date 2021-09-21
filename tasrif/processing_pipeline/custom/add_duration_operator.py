"""
Operator to aggregate column features based on a column
"""
from datetime import datetime

from tasrif.processing_pipeline import ProcessingOperator


class AddDurationOperator(ProcessingOperator):
    """

      Given a 2D dataframe representing a timeseries where each row represents a time event, this operator
      will add a new feature duration to compute duration.

      Examples
      --------

      >>> import pandas as pd
      >>>
      >>> from tasrif.processing_pipeline.custom import AddDurationOperator
      >>>
      >>> df0 = pd.DataFrame([[1, "2020-05-01 00:00:00", 1], [1, "2020-05-01 01:00:00", 1],
      >>> [1, "2020-05-01 03:00:00", 2], [2, "2020-05-02 00:00:00", 1],[2, "2020-05-02 01:00:00", 1]],
      >>>               columns=['logId', 'timestamp', 'sleep_level'])
      >>> df0['timestamp'] = pd.to_datetime(df0['timestamp'])
      >>>
      >>> operator = AddDurationOperator(
      >>>    groupby_feature_names="logId",
      >>>    date_feature_name="timestamp",
      >>>    duration_feature_name="duration")
      >>> df0 = operator.process(df0)
      >>>
      >>> print(df0)
      [   logId           timestamp  sleep_level        duration
      0      1 2020-05-01 00:00:00            1 0 days 00:00:00
      1      1 2020-05-01 01:00:00            1 0 days 01:00:00
      2      1 2020-05-01 03:00:00            2 0 days 02:00:00
      3      2 2020-05-02 00:00:00            1 0 days 00:00:00
      4      2 2020-05-02 01:00:00            1 0 days 01:00:00]

    """
    def __init__(self,
                 groupby_feature_names,
                 date_feature_name="timestamp",
                 duration_feature_name="duration"):
        """Creates a new instance of AddDurationOperator

        Args:
            groupby_feature_names (str):
                Name of the feature to identify related timestamp series
            date_feature_name (str):
                Name of the feature respresenting the timestamp
            duration_feature_name (str):
                Name of the feature representing the duration

        """
        super().__init__()
        self.groupby_feature_names = groupby_feature_names
        self.date_feature_name = date_feature_name
        self.duration_feature_name = duration_feature_name

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
            data_frame[self.duration_feature_name] = data_frame[
                self.date_feature_name].sub(
                    data_frame[self.date_feature_name].shift())
            now = datetime.now()
            zero_duration = now - now
            # Change the duration of the first entry of every sleep log  group to zero
            data_frame.loc[data_frame.groupby(self.groupby_feature_names)[
                self.duration_feature_name].head(1).index,
                           self.duration_feature_name] = zero_duration

            processed.append(data_frame)

        return processed
