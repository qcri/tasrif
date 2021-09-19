"""
Operator to upsample a timeseries based dataframe in a distributed way
"""
from tasrif.processing_pipeline import ProcessingOperator


class DistributedUpsampleOperator(ProcessingOperator):
    """

    Upsamples the dataframe based assuming the index

    **Example**:

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.custom import DistributedUpsampleOperator
    >>> df = pd.DataFrame([
    >>>     ["2020-05-01", 16.5],
    >>>     ["2020-05-02", 19.1],
    >>>     ['2020-05-03', 0]],
    >>>     columns=['timestamp', 'sedentary_hours'])
    >>>
    >>> df['timestamp'] = pd.to_datetime(df['timestamp'])
    >>> df = df.set_index('timestamp')
    >>> op = DistributedUpsampleOperator('6h')
    >>> df = op.process(df)
    >>>   [            sleep_level
    >>>   timestamp
    >>>   2020-05-01     1.333333
    >>>   2020-05-02     1.000000]
    [                     sedentary_hours
    timestamp
    2020-05-01 00:00:00            4.125
    2020-05-01 06:00:00            4.125
    2020-05-01 12:00:00            4.125
    2020-05-01 18:00:00            4.125
    2020-05-02 00:00:00            4.775
    2020-05-02 06:00:00            4.775
    2020-05-02 12:00:00            4.775
    2020-05-02 18:00:00            4.775
    2020-05-03 00:00:00            0.000]

    """

    def _distributed_upsample(self, data_frame):
        freq = self.rule
        data_frame = data_frame[~data_frame.index.duplicated(keep="first")]
        data_frame["group"] = data_frame.index
        data_frame["count"] = data_frame.resample(
            freq).ffill()["group"].value_counts()
        data_frame = data_frame.resample(freq).ffill()
        data_frame = data_frame.drop("group", axis=1)
        data_frame = data_frame.div(data_frame["count"], axis=0)
        data_frame = data_frame.drop("count", axis=1)
        return data_frame

    def __init__(self, rule):
        """Creates a new instance of ResampleOperator

        Args:
            rule (ruleDateOffset, Timedelta, str):
                The offset string or object representing target conversion.

        """
        super().__init__()
        self.rule = rule

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
            data_frame = self._distributed_upsample(data_frame)
            processed.append(data_frame)

        return processed
