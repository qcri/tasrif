"""
Groupby Operator
"""
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin


class GroupbyOperator(InputsAreDataFramesValidatorMixin, PandasOperator):
    """

    Examples
    --------
    >>> import pandas as pd
    >>> import numpy as np
    >>> from tasrif.processing_pipeline.pandas import GroupbyOperator
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
    >>> operator = GroupbyOperator(by='ActivityTime')
    >>> df = operator.process(df)[0]
    >>>
    >>> print(df.get_group(1))
    >>> print(df.get_group(2))
    >>> print(df.get_group(3))
    Id        ActivityTime  Calories
    0   1 2016-03-12 01:00:00        10
    1   1 2016-03-12 04:00:00       250
    2   1 2016-03-12 06:00:00        30
    3   1 2016-03-12 20:00:00        10
    4   1 2016-03-12 23:00:00        23
    ...
    Id        ActivityTime  Calories
    5   2 2016-03-12 00:05:00        20
    6   2 2016-03-12 19:06:00       120
    7   2 2016-03-12 21:07:00       100
    8   2 2016-03-12 23:08:00        50
    Id        ActivityTime  Calories
    9   3 2016-03-12 10:00:00       300

    """

    def __init__(self, selector=None, **kwargs):
        """Creates a new instance of GroupbyOperator

        Args:
            selector:
                selects the columns of a groupby object
            **kwargs:
                Arguments to pandas pd.groupby function

        """

        self.selector = selector
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
            if self.selector:
                data_frame = data_frame.groupby(**self.kwargs)[self.selector]
            else:
                data_frame = data_frame.groupby(**self.kwargs)

            processed.append(data_frame)

        return processed
