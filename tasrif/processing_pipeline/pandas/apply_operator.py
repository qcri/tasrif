"""
Merge multiple dataframes into a single one.
"""
from tasrif.processing_pipeline import ProcessingOperator


class ApplyOperator(ProcessingOperator):
    """Merge different datasets based on Pandas merge method.

    Parameters
    ----------

    Returns
    -------

    Examples:
    ---------
    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.pandas import ApplyOperator
    >>> df = pd.DataFrame([
    >>>     [1, "2020-05-01 00:00:00", 1],
    >>>     [1, "2020-05-01 01:00:00", 1], 
    >>>     [1, "2020-05-01 03:00:00", 2], 
    >>>     [2, "2020-05-02 00:00:00", 1],
    >>>     [2, "2020-05-02 01:00:00", 1]],
    >>>     columns=['logId', 'timestamp', 'sleep_level'])
    >>> 
    >>> df['timestamp'] = pd.to_datetime(df['timestamp'])
    >>> op = ApplyOperator(lambda df: df['sleep_level'] + 1, axis=1)
    >>> op.process(df)[0]
    0    2
    1    2
    2    3
    3    2
    4    2
    dtype: int64
    """

    def __init__(self, func, **kwargs):
        """Merge different datasets on a common feature defined by ``on``.

        Parameters
        ----------
        data_frames:
          Variable number of pandas dataframes to be processed

        **kwargs:
          key word arguments passed to pandas DataFrame.merge method


        """
        self.func = func
        self.kwargs = kwargs
        super().__init__()

    def process(self, *data_frames):
        """Apply a function along an axis of the DataFrame.

        Returns
        -------
        data_frames
            Resulting dataframes after the apply function.
        """
        # Gets one single
        processed = []
        for data_frame in data_frames:
            data_frame = data_frame.apply(self.func, **self.kwargs)
            processed.append(data_frame)
        return processed
