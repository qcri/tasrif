"""
Drop NaN values from one or more dataframes
"""
from tasrif.processing_pipeline import ProcessingOperator


class DropNAOperator(ProcessingOperator):
    """
    Parameters
    ----------

    Raises
    ------

    Examples
    --------

    >>> import pandas as pd
    >>> import numpy as np
    >>> from tasrif.processing_pipeline import DropNAOperator
    >>>
    >>> df0 = pd.DataFrame([['tom', 10], ['nick', 15], ['juli', 14]])
    >>> df1 = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
    ...                   "toy": [np.nan, 'Batmobile', 'Bullwhip'],
    ...                   "born": [pd.NaT, pd.Timestamp("1940-04-25"),
    ...                            pd.NaT]})
    >>>operator = DropNAOperator(axis=0)
    >>>df0, df1 = operator.process(df0, df1)
    >>>print(df0)
    >>>print(df1)
    .     0   1
    0   tom  10
    1  nick  15
    2  juli  14
    .    name        toy       born
    1  Batman  Batmobile 1940-04-25
    """

    def __init__(self, **kwargs):
        """
        Initializes the operator

        \\*\\*kwargs:
          key word arguments passed to pandas DataFrame.dropna method
        """
        self.kwargs = kwargs
        super().__init__()

    def process(self, *data_frames):
        """Process the passed data using the processing configuration specified
        in the constructor

        data_frames:
          Variable number of pandas dataframes to be processed

        Returns
        -------
        data_frames
            Processed data frames
        """

        processed = []
        for dataframe in data_frames:
            dataframe = dataframe.dropna(**self.kwargs)
            processed.append(dataframe)

        return tuple(processed)
