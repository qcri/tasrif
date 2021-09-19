"""
Set the DataFrame index using existing columns.

Set the DataFrame index (row labels) using one or more existing columns or arrays (of the correct length).
The index can replace the existing index or expand on it.
"""
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin


class SetIndexOperator(InputsAreDataFramesValidatorMixin, PandasOperator):
    """
    Parameters
    ----------

    Raises
    ------

    Examples
    --------
    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.pandas import SetIndexOperator
    >>> df = pd.DataFrame([
    ...     [1, "2020-05-01 00:00:00", 1],
    ...     [1, "2020-05-01 01:00:00", 1],
    ...     [1, "2020-05-01 03:00:00", 2],
    ...     [2, "2020-05-02 00:00:00", 1],
    ...     [2, "2020-05-02 01:00:00", 1]],
    ...     columns=['logId', 'timestamp', 'sleep_level'])
    >>> df
    logId	timestamp	sleep_level
    0	1	2020-05-01 00:00:00	1
    1	1	2020-05-01 01:00:00	1
    2	1	2020-05-01 03:00:00	2
    3	2	2020-05-02 00:00:00	1
    4	2	2020-05-02 01:00:00	1

    >>> op = SetIndexOperator('timestamp')
    >>> op.process(df)
    [                     logId  sleep_level
    timestamp
    2020-05-01 00:00:00      1            1
    2020-05-01 01:00:00      1            1
    2020-05-01 03:00:00      1            2
    2020-05-02 00:00:00      2            1
    2020-05-02 01:00:00      2            1]

    """
    def __init__(self, keys, **kwargs):
        """
        Initializes the operator.

        Args:
            keys (str or list):
                This parameter can be either a single column key,
                a single array of the same length as the calling DataFrame,
                or a list containing an arbitrary combination of column keys and arrays.
            **kwargs: key word arguments passed to pandas DataFrame.dropna method

        """
        self.keys = keys
        super().__init__(kwargs)
        self.kwargs = kwargs

    def _process(self, *data_frames):
        """Process the passed data using the processing configuration specified
        in the constructor

        Args:
            data_frames (list of pd.DataFrame):
                Variable number of arrays of python dictionaries (representing JSON data) to be processed

        Returns:
            data_frames (list of pd.DataFrame):
                Resulting dataframes.
        """

        processed = []
        for dataframe in data_frames:
            dataframe = dataframe.set_index(self.keys, **self.kwargs)
            dataframe.sort_index(inplace=True)
            processed.append(dataframe)

        return processed
