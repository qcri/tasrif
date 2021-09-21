"""
Operator to extract features from a duration column
"""

from datetime import timedelta
from tasrif.processing_pipeline import ProcessingOperator


class CategorizeDurationOperator(ProcessingOperator):
    """

      Given a 2D dataframe representing a timeseries
      where each row represents a time duration, this operator
      will add a new feature(s) that represent a categorization of the duration.
      The categorization specification is provided in the constructor.

      Examples
      --------

        >>> from datetime import timedelta
        >>> import numpy as np
        >>> import pandas as pd
        >>> import seaborn as sns
        >>> from tasrif.processing_pipeline.custom import CategorizeDurationOperator
        >>>
        >>>
        >>> dates = pd.date_range('2016-12-31', '2017-01-08', freq='D').to_series()
        >>> df = pd.DataFrame()
        >>> df["Date"] = dates
        >>> df['Last_Date'] = df['Date'].apply(lambda x: x + timedelta(days=np.random.randint(3),
        >>>                                                            hours=np.random.randint(24),
        >>>                                                            minutes=np.random.randint(60)))
        >>> df['Duration'] = df['Last_Date'] - df['Date']
        >>> df['Steps'] = np.random.randint(1000,25000, size=len(df))
        >>> df['Calories'] = np.random.randint(1800,3000, size=len(df))
        >>>
        >>> # %%
        >>> df # pylint: disable=pointless-statement
        >>>
        >>>
            Date    Last_Date   Duration    Steps   Calories
        2016-12-31  2016-12-31  2016-12-31 05:01:00     0 days 05:01:00     10858   1852
        2017-01-01  2017-01-01  2017-01-01 23:51:00     0 days 23:51:00     19802   2126
        2017-01-02  2017-01-02  2017-01-03 03:32:00     1 days 03:32:00     1924    2201
        2017-01-03  2017-01-03  2017-01-04 01:31:00     1 days 01:31:00     3393    1935
        2017-01-04  2017-01-04  2017-01-04 03:44:00     0 days 03:44:00     8177    2833
        2017-01-05  2017-01-05  2017-01-06 14:24:00     1 days 14:24:00     21838   2893
        2017-01-06  2017-01-06  2017-01-08 00:53:00     2 days 00:53:00     5671    2095
        2017-01-07  2017-01-07  2017-01-09 21:26:00     2 days 21:26:00     6792    2350
        2017-01-08  2017-01-08  2017-01-09 05:21:00     1 days 05:21:00     24555   2425


        >>> df1 = df.copy()
        >>> operator = CategorizeDurationOperator(duration_feature_name="Duration", category_definition="day")
        >>> df1 = operator.process(df1)[0]
        >>> df1 # pylint: disable=pointless-statement
        >>>
            Date    Last_Date   Duration    Steps   Calories    day_delta
        2016-12-31  2016-12-31  2016-12-31 05:01:00     0 days 05:01:00     10858   1852    0
        2017-01-01  2017-01-01  2017-01-01 23:51:00     0 days 23:51:00     19802   2126    0
        2017-01-02  2017-01-02  2017-01-03 03:32:00     1 days 03:32:00     1924    2201    1
        2017-01-03  2017-01-03  2017-01-04 01:31:00     1 days 01:31:00     3393    1935    1
        2017-01-04  2017-01-04  2017-01-04 03:44:00     0 days 03:44:00     8177    2833    0
        2017-01-05  2017-01-05  2017-01-06 14:24:00     1 days 14:24:00     21838   2893    1
        2017-01-06  2017-01-06  2017-01-08 00:53:00     2 days 00:53:00     5671    2095    2
        2017-01-07  2017-01-07  2017-01-09 21:26:00     2 days 21:26:00     6792    2350    2
        2017-01-08  2017-01-08  2017-01-09 05:21:00     1 days 05:21:00     24555   2425    1

    """
    def __init__(self,
                 duration_feature_name="duration",
                 category_definition="minute"):
        """Creates a new instance of CategorizeDurationOperator

        Args:
            duration_feature_name (str):
                Name of the feature to identify related time delta series
            category_definition (str, list[str]): str or array of str
                Value is one of "day", "hour" or "minutes" to categorize based on
                number of days, hours of the minutes::

                    [
                        "day"
                    ]

                Array of dictionary customized column names are desired::

                    [
                        {"day": "day_of_week"},
                    ]
        """
        super().__init__()
        self.duration_feature_name = duration_feature_name
        self.category_definition = category_definition

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
            if isinstance(self.category_definition, str):
                function = getattr(self, f"_{self.category_definition}")
                data_frame = function(data_frame)
            elif isinstance(self.category_definition, list):
                for category in self.category_definition:
                    data_frame = self._process_category(category, data_frame)
            processed.append(data_frame)

        return processed

    def _process_category(self, category, data_frame):
        if isinstance(category, str):
            function = getattr(self, f"_{category}")
            data_frame = function(data_frame)
        else:
            for k, val in category.items():
                if k != "values":
                    function = getattr(self, f"_{k}")
                    data_frame = function(data_frame, val, category.get("values"))
                    break
        return data_frame


    def _day(self, data_frame, column_name="day_delta", values=None):
        data_frame[column_name] = data_frame[self.duration_feature_name]//timedelta(days=1)
        if values:
            data_frame[column_name] = data_frame[column_name].apply(lambda x: values[x])
        return data_frame

    def _hour(self, data_frame, column_name="hour_delta", values=None):
        data_frame[column_name] = data_frame[self.duration_feature_name]//timedelta(hours=1)
        if values:
            data_frame[column_name] = data_frame[column_name].apply(lambda x: values[x])
        return data_frame

    def _minute(self, data_frame, column_name="minute_delta", values=None):
        data_frame[column_name] = data_frame[self.duration_feature_name]//timedelta(minutes=1)
        if values:
            data_frame[column_name] = data_frame[column_name].apply(lambda x: values[x])
        return data_frame
