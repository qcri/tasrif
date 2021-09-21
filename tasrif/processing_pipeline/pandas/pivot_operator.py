"""
Pivots a dataframe
"""
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin

class PivotOperator(InputsAreDataFramesValidatorMixin, PandasOperator):
    """
    Pivots a dataframe.

    Examples
    --------
    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.pandas import PivotOperator
    >>> df = pd.DataFrame([
    >>>     [1, "2020-05-01 00:00:00", 1],
    >>>     [1, "2020-05-01 01:00:00", 1],
    >>>     [1, "2020-05-01 03:00:00", 2],
    >>>     [2, "2020-05-02 00:00:00", 1],
    >>>     [2, "2020-05-02 01:00:00", 1]],
    >>>     columns=['logId', 'timestamp', 'sleep_level'])
    >>>
    >>> df['timestamp'] = pd.to_datetime(df['timestamp'])
    >>> op = PivotOperator(index='timestamp', columns='logId', values='sleep_level')
    >>> op.process(df)[0]
    logId   1   2
    timestamp
    2020-05-01 00:00:00   1.0   NaN
    2020-05-01 01:00:00   1.0   NaN
    2020-05-01 03:00:00   2.0   NaN
    2020-05-02 00:00:00   NaN   1.0
    2020-05-02 01:00:00   NaN   1.0

    """

    def __init__(self, **kwargs):
        """Creates a new instance of PivotOperator

        Args:
            **kwargs: Arguments to pandas pivot function

        """
        super().__init__(kwargs)
        self.kwargs = kwargs


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
            data_frame = data_frame.pivot(**self.kwargs)
            processed.append(data_frame)

        return processed
