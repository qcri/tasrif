"""
Normalize semi-structured JSON data into a flat table.
"""
import pandas as pd
from tasrif.processing_pipeline import PandasOperator

class JsonNormalizeOperator(PandasOperator):
    """

    Examples
    --------

    >>> from tasrif.processing_pipeline.pandas import JsonNormalizeOperator
    >>> data = [
    ...     {
    ...         "date": "2019-05-02",
    ...         "main_sleep": True,
    ...         "data": {
    ...             "total_sleep": 365,
    ...             "time_series": [{
    ...                   "dateTime" : "2019-05-02T00:18:00.000",
    ...                   "level" : "light",
    ...                   "seconds" : 2130
    ...                 },{
    ...                   "dateTime" : "2019-05-02T00:53:30.000",
    ...                   "level" : "deep",
    ...                   "seconds" : 540
    ...                 },{
    ...                   "dateTime" : "2019-05-02T01:02:30.000",
    ...                   "level" : "light",
    ...                   "seconds" : 870
    ...                 },{
    ...                   "dateTime" : "2019-05-02T01:17:00.000",
    ...                   "level" : "rem",
    ...                   "seconds" : 660
    ...                 },{
    ...                   "dateTime" : "2019-05-02T01:28:00.000",
    ...                   "level" : "light",
    ...                   "seconds" : 1230
    ...                 },{
    ...                   "dateTime" : "2019-05-02T01:48:30.000",
    ...                   "level" : "wake",
    ...                   "seconds" : 210
    ...                 }]
    ...             }
    ...     },
    ...     {
    ...         "date": "2019-04-29",
    ...         "main_sleep": True,
    ...         "data": {
    ...               "total_sleep": 456,
    ...               "time_series": [{
    ...               "dateTime" : "2019-04-29T23:46:00.000",
    ...               "level" : "wake",
    ...               "seconds" : 300
    ...             },{
    ...               "dateTime" : "2019-04-29T23:51:00.000",
    ...               "level" : "light",
    ...               "seconds" : 660
    ...             },{
    ...               "dateTime" : "2019-04-30T00:02:00.000",
    ...               "level" : "deep",
    ...               "seconds" : 450
    ...             },{
    ...               "dateTime" : "2019-04-30T00:09:30.000",
    ...               "level" : "light",
    ...               "seconds" : 2070
    ...             }]
    ...         }
    ...     }
    ... ]
    >>>
    >>> op = JsonNormalizeOperator(record_path=['data', 'time_series'],
    ...                            meta=['date', 'main_sleep', ['data', 'total_sleep']])
    >>>
    >>> df = op.process(data)
    >>> print(df)
    [                  dateTime  level  seconds        date main_sleep      data.total_sleep
    0  2019-05-02T00:18:00.000  light     2130  2019-05-02       True       365
    1  2019-05-02T00:53:30.000   deep      540  2019-05-02       True       365
    2  2019-05-02T01:02:30.000  light      870  2019-05-02       True       365
    3  2019-05-02T01:17:00.000    rem      660  2019-05-02       True       365
    4  2019-05-02T01:28:00.000  light     1230  2019-05-02       True       365
    5  2019-05-02T01:48:30.000   wake      210  2019-05-02       True       365
    6  2019-04-29T23:46:00.000   wake      300  2019-04-29       True       456
    7  2019-04-29T23:51:00.000  light      660  2019-04-29       True       456
    8  2019-04-30T00:02:00.000   deep      450  2019-04-29       True       456
    9  2019-04-30T00:09:30.000  light     2070  2019-04-29       True       456  ]

    """

    def __init__(self, **kwargs):
        """
        Initializes the operator

        Args:
            **kwargs:
              key word arguments passed to pandas DataFrame.dropna method
        """
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
                Processed data frames
        """

        processed = []
        for dataframe in data_frames:
            dataframe = pd.json_normalize(dataframe, **self.kwargs)
            processed.append(dataframe)

        return processed
