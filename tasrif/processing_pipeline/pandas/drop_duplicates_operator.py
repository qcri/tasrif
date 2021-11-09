
"""
Remove duplicate values from one or more dataframes.
"""
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin


class DropDuplicatesOperator(InputsAreDataFramesValidatorMixin, PandasOperator):
    """
    Remove duplicate rows from one or more dataframes.

    Examples
    --------

    >>> import pandas as pd
    >>> import numpy as np
    >>>
    >>> from tasrif.processing_pipeline.pandas import DropDuplicatesOperator
    >>>
    >>> df0 = pd.DataFrame([['Tom', 10], ['Alfred', 15], ['Alfred', 18], ['Juli', 14]], columns=['name', 'score'])
    >>> df1 = pd.DataFrame({"name": ['Alfred', 'juli', 'Tom', 'Ali'],
    ...                   "height": [np.nan, 155, 159, 165],
    ...                   "born": [pd.NaT, pd.Timestamp("2010-04-25"), pd.NaT,
    ...                            pd.NaT]})
    >>>
    >>> operator = DropDuplicatesOperator(subset='name')
    >>> df0, df1 = operator.process(df0, df1)
    >>>
    >>> print(df0)
    >>> print(df1)
         name  score
    0     Tom     10
    1  Alfred     15
    3    Juli     14
         name  height       born
    0  Alfred     NaN        NaT
    1    juli   155.0 2010-04-25
    2     Tom   159.0        NaT
    3     Ali   165.0        NaT
    """

    def __init__(self, **kwargs):
        """
        Initializes the operator

        Args:
            **kwargs:
              key word arguments passed to pandas DataFrame.drop_duplicates method
        """
        super().__init__(kwargs)
        self.kwargs = kwargs

    def _process(self, *data_frames):
        """Process the passed data using the processing configuration specified
        in the constructor

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            data_frames
                Processed data frames
        """

        processed = []
        for dataframe in data_frames:
            dataframe = dataframe.drop_duplicates(**self.kwargs)
            processed.append(dataframe)

        return tuple(processed)
