"""
Cast a pandas object to a specified dtype dtype.
"""
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin

class AsTypeOperator(InputsAreDataFramesValidatorMixin, PandasOperator):
    """
    Cast a pandas object to a specified dtype dtype.

    Examples
    --------
    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.pandas import AsTypeOperator
    >>> df = pd.DataFrame([
    ...     [1, "2020-05-01 00:00:00", "1", "3"],
    ...     [1, "2020-05-01 01:00:00", "1", "5" ],
    ...     [1, "2020-05-01 03:00:00", "2", "3"],
    ...     [2, "2020-05-02 00:00:00", "1", "10"],
    ...     [2, "2020-05-02 01:00:00", "1", "0"]],
    ...     columns=['logId', 'timestamp', 'sleep_level', 'awake_count'])
    >>>  df.dtypes
    logId           int64
    timestamp      object
    sleep_level    object
    awake_count    object
    dtype: object

    >>> op = AsTypeOperator({'sleep_level': 'int32', 'awake_count' : 'int32'})
    >>> df1 = op.process(df)
    >>> df1[0].dtypes
    logId           int64
    timestamp      object
    sleep_level     int32
    awake_count     int32
    dtype: object

    """

    def __init__(self, dtype, **kwargs):
        """Creates a new instance of AddDurationOperator


        Args:
            dtype: dtypedata type, or dict of column name -> data type
                   Use a numpy.dtype or Python type to cast entire pandas object to the same type.
                   Alternatively, use {col: dtype, …}, where col is a column label and dtype is a numpy.dtype or
                   Python type to cast one or more of the DataFrame’s columns to column-specific types.
            **kwargs: Arguments to pandas pd.astype function

        """
        self.dtype = dtype
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
            data_frame = data_frame.astype(self.dtype, **self.kwargs)
            processed.append(data_frame)

        return processed
