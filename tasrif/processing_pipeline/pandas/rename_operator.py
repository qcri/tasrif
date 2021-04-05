"""
Resets the DataFrame index.

"""
from tasrif.processing_pipeline import ProcessingOperator


class RenameOperator(ProcessingOperator):
    """
    Parameters
    ----------

    Raises
    ------

    Examples
    --------
    >>> import pandas as pd
    >>> import numpy as np
    >>> 
    >>> from tasrif.processing_pipeline.pandas import RenameOperator
    >>> 
    >>> 
    >>> 
    >>> df = pd.DataFrame([
    >>>     [1,'2016-03-12 01:00:00',10],
    >>>     [1,'2016-03-12 04:00:00',250],
    >>>     [1,'2016-03-12 06:00:00',30],
    >>>     [1,'2016-03-12 20:00:00',10],
    >>>     [1,'2016-03-12 23:00:00',23],
    >>>     [2,'2016-03-12 00:05:00',20],
    >>>     [2,'2016-03-12 19:06:00',120],
    >>>     [2,'2016-03-12 21:07:00',100],
    >>>     [2,'2016-03-12 23:08:00',50],
    >>>     [3,'2016-03-12 10:00:00',300]
    >>> ], columns=['Id', 'ActivityTime', 'Calories'])
    >>> 
    >>> df['ActivityTime'] = pd.to_datetime(df['ActivityTime'])
    >>> 
    >>> 
    >>> operator = RenameOperator(columns={"ActivityTime": "time"})
    >>> df = operator.process(df)[0]
    >>> df

    Id  time    Calories
    0   1   2016-03-12 01:00:00     10
    1   1   2016-03-12 04:00:00     250
    2   1   2016-03-12 06:00:00     30
    3   1   2016-03-12 20:00:00     10
    4   1   2016-03-12 23:00:00     23
    5   2   2016-03-12 00:05:00     20
    6   2   2016-03-12 19:06:00     120
    7   2   2016-03-12 21:07:00     100
    8   2   2016-03-12 23:08:00     50
    9   3   2016-03-12 10:00:00     300


    """

    def __init__(self, **kwargs):
        """
        Initializes the operator

        **kwargs:
          key word arguments passed to pandas DataFrame.reset_index method
        """
        
        self.kwargs = kwargs
        super().__init__()

    def process(self, *data_frames):
        """Process the passed data using the processing configuration specified
        in the constructor

        data_frames:
          Variable number of arrays of python dictionaries (representing JSON data) to be processed

        Returns
        -------
        data_frames
            Processed data frames
        """

        processed = []
        for dataframe in data_frames:
            dataframe = dataframe.rename(**self.kwargs)
            processed.append(dataframe)

        return processed
