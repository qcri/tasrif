"""
Resets the DataFrame index.

"""
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin


class ResetIndexOperator(InputsAreDataFramesValidatorMixin, PandasOperator):
    """

    Examples
    --------
    >>> import pandas as pd
    >>> import numpy as np
    >>>
    >>> from tasrif.processing_pipeline.pandas import ResetIndexOperator
    >>>
    >>>
    >>>
    >>> df = pd.DataFrame([
    ...     [1,'2016-03-12 01:00:00',10],
    ...     [1,'2016-03-12 04:00:00',250],
    ...     [1,'2016-03-12 06:00:00',30],
    ...     [1,'2016-03-12 20:00:00',10],
    ...     [1,'2016-03-12 23:00:00',23],
    ...     [2,'2016-03-12 00:05:00',20],
    ...     [2,'2016-03-12 19:06:00',120],
    ...     [2,'2016-03-12 21:07:00',100],
    ...     [2,'2016-03-12 23:08:00',50],
    ...     [3,'2016-03-12 10:00:00',300]
    ... ], columns=['Id', 'ActivityTime', 'Calories'])
    >>>
    >>> df['ActivityTime'] = pd.to_datetime(df['ActivityTime'])
    >>>
    >>>
    >>> df = df.set_index('ActivityTime')
    >>> operator = ResetIndexOperator()
    >>> df = operator.process(df)[0]
    >>> df
    ActivityTime    Id  Calories
    0   2016-03-12 01:00:00     1   10
    1   2016-03-12 04:00:00     1   250
    2   2016-03-12 06:00:00     1   30
    3   2016-03-12 20:00:00     1   10
    4   2016-03-12 23:00:00     1   23
    5   2016-03-12 00:05:00     2   20
    6   2016-03-12 19:06:00     2   120
    7   2016-03-12 21:07:00     2   100
    8   2016-03-12 23:08:00     2   50
    9   2016-03-12 10:00:00     3   300

    """

    def __init__(self, **kwargs):
        """
        Initializes the operator

        Args:
            **kwargs:
              key word arguments passed to pandas DataFrame.reset_index method
        """
        super().__init__(kwargs)
        self.kwargs = kwargs

    def _process(self, *data_frames):
        """Process the passed data using the processing configuration specified
        in the constructor

        Args:
            *data_frames (list of pd.DataFrame):
                Variable number of arrays of python dictionaries (representing JSON data) to be processed

        Returns:
            data_frames (list of pd.DataFrame)
                Processed data frames
        """

        processed = []
        for dataframe in data_frames:
            dataframe = dataframe.reset_index(**self.kwargs)
            processed.append(dataframe)

        return processed
