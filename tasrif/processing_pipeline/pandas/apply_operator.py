"""
Applies on a dataframe
"""
from tasrif.processing_pipeline import ProcessingOperator

class ApplyOperator(ProcessingOperator):
    """
    Applies on a dataframe.

    Examples
    --------

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.pandas import ApplyOperator
    >>> df = pd.DataFrame([
    ...     [1, 1, 3],
    ...     [1, 1, 5],
    ...     [1, 2, 3],
    ...     [2, 1, 10],
    ...     [2, 1, 0]],
    ...     columns=['logId', 'sleep_level', 'awake_count'])
    >>>
    >>> df = df.set_index('logId')
    >>> op = ApplyOperator(lambda df: df['sleep_level'] + df['awake_count'], axis=1)
    >>> df1 = op.process(df)
    >>> df1[0]
    logId
    1     4
    1     6
    1     5
    2    11
    2     1
    dtype: int64

    """

    def __init__(self, func, **kwargs):
        """Creates a new instance of ApplyOperator

        Args:
            func (function):
                Function to apply to each column or row.
            **kwargs: Arguments to pandas apply function

        """
        self.func = func
        self.kwargs = kwargs


    def process(self, *data_frames):
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
            data_frame = data_frame.apply(self.func, **self.kwargs)
            processed.append(data_frame)

        return processed
