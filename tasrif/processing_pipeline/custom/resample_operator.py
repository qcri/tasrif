"""
Operator to resample a timeseries based dataframe
"""
from tasrif.processing_pipeline import ProcessingOperator


class ResampleOperator(ProcessingOperator):
    """

    Group and aggregate rows in 2D data frame based on a column feature.
    This operator works on a 2D data frames where the
    columns represent the features. The returned data frame contains aggregated values as the column features together
    with the base feature used for grouping.

    Examples
    --------

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.custom import ResampleOperator
    >>> df = pd.DataFrame([
    >>>     [1, "2020-05-01 00:00:00", 1],
    >>>     [1, "2020-05-01 01:00:00", 1],
    >>>     [1, "2020-05-01 03:00:00", 2],
    >>>     [2, "2020-05-02 00:00:00", 1],
    >>>     [2, "2020-05-02 01:00:00", 1]],
    >>>     columns=['logId', 'timestamp', 'sleep_level'])
    >>>
    >>> df['timestamp'] = pd.to_datetime(df['timestamp'])
    >>> df = df.set_index('timestamp')
    >>> op = ResampleOperator('D', {'sleep_level': 'mean'})
    >>> op.process(df)
    [            sleep_level
    timestamp
    2020-05-01     1.333333
    2020-05-02     1.000000]

    """
    def __init__(self, rule, aggregation_definition, **resample_args):
        """Creates a new instance of ResampleOperator

        Args:
            rule (ruleDateOffset, Timedelta, str):
                The offset string or object representing target conversion.
            aggregation_definition (dict, str):
                Dictionary containing feature to aggregation functions mapping.
                function defining the aggregation behavior ('sum', 'mean', 'ffill', etc.)
            **resample_args:
                key word arguments passed to pandas DataFrame.resample method
        """
        super().__init__()
        self.rule = rule
        self.aggregation_definition = aggregation_definition
        self.resample_args = resample_args

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
            resampler = data_frame.resample(self.rule, **self.resample_args)
            if isinstance(self.aggregation_definition, dict):

                data_frame = resampler.agg(self.aggregation_definition)
            else:
                data_frame = getattr(resampler, self.aggregation_definition)()
            processed.append(data_frame)

        return processed
