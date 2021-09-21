"""
Operator to aggregate column features based on a column
"""

from calendar import monthrange
import numpy as np

from tasrif.processing_pipeline import ProcessingOperator


class EncodeCyclicalFeaturesOperator(ProcessingOperator):
    """
    This method converts datetime pandas series to machine learning acceptable format.
    It extracts year, month, day, hour, and minute from the datetime object.
    The method returns a dataframe, as shown in below example.

    >>> import numpy as np
    >>> import pandas as pd
    >>> import seaborn as sns
    >>> from tasrif.processing_pipeline.custom import EncodeCyclicalFeaturesOperator
    >>>
    >>>
    >>> dates = pd.date_range('2016-12-31', '2017-01-08', freq='D').to_series()
    >>> df = pd.DataFrame()
    >>> df["Date"] = dates
    >>> df['Steps'] = np.random.randint(1000,25000, size=len(df))
    >>> df['Calories'] = np.random.randint(1800,3000, size=len(df))
    >>>
    >>> df3 = df.copy()
    >>> operator = EncodeCyclicalFeaturesOperator(date_feature_name="Date",
    >>>                                           category_definition=["day", "day_in_month"])
    >>> df3 = operator.process(df3)[0]
    Date    Steps   Calories    day_sin     day_cos     day_in_month_sin    day_in_month_cos
    2016-12-31  2016-12-31  3906    1910    -0.974928   -0.222521   -2.449294e-16   1.000000
    2017-01-01  2017-01-01  7079    2909    -0.781831   0.623490    2.012985e-01    0.979530
    2017-01-02  2017-01-02  19877   2503    0.000000    1.000000    3.943559e-01    0.918958
    2017-01-03  2017-01-03  12873   2298    0.781831    0.623490    5.712682e-01    0.820763
    2017-01-04  2017-01-04  19647   2438    0.974928    -0.222521   7.247928e-01    0.688967
    2017-01-05  2017-01-05  17891   2704    0.433884    -0.900969   8.486443e-01    0.528964
    2017-01-06  2017-01-06  16573   2825    -0.433884   -0.900969   9.377521e-01    0.347305
    2017-01-07  2017-01-07  16222   2752    -0.974928   -0.222521   9.884683e-01    0.151428
    2017-01-08  2017-01-08  9702    2772    -0.781831   0.623490    9.987165e-01    -0.050649

    """
    def __init__(self, date_feature_name="date", category_definition='hour'):
        """Creates a new instance of EncodeCyclicalFeaturesOperator

        Args:
            date_feature_name : str
                Name of the feature to identify related timestamp series
            category_definition : str or array of str or dict
                Value is one of "day", "month" to categorize based on
                day of the week, month of the year or hijri month
                Array of these values if multiple categorizations are desired.::

                    [
                        "days", "month"
                    ]

                Array of dictionary customized column names are desired::

                    [
                        {"days": "day_of_week"},
                        {"month", "calendar_month}
                    ]
        """
        super().__init__()
        self.date_feature_name = date_feature_name
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
                function = getattr(self, f"_{k}")
                data_frame = function(data_frame, val)

        return data_frame

    def _month(self, data_frame, column_name="month"):
        # retain cyclic nature of time
        data_frame[column_name + '_sin'] = np.sin(
            2 * np.pi * data_frame[self.date_feature_name].dt.month / 12)
        data_frame[column_name + '_cos'] = np.cos(
            2 * np.pi * data_frame[self.date_feature_name].dt.month / 12)
        return data_frame

    def _day_in_month(self, data_frame, column_name="day_in_month"):
        # some months have 28, 29, 30, and 31 days
        days_in_month = data_frame[self.date_feature_name].apply(
            lambda x: monthrange(x.year, x.month)[1])
        data_frame[column_name + '_sin'] = np.sin(
            2 * np.pi * data_frame[self.date_feature_name].dt.day /
            days_in_month)
        data_frame[column_name + '_cos'] = np.cos(
            2 * np.pi * data_frame[self.date_feature_name].dt.day /
            days_in_month)
        return data_frame

    def _day(self, data_frame, column_name="day"):
        data_frame[column_name + '_sin'] = np.sin(
            2 * np.pi * data_frame[self.date_feature_name].dt.dayofweek / 7)
        data_frame[column_name + '_cos'] = np.cos(
            2 * np.pi * data_frame[self.date_feature_name].dt.dayofweek / 7)
        return data_frame

    def _hour(self, data_frame, column_name="hour"):
        # retain cyclic nature of time
        data_frame[column_name + '_sin'] = np.sin(
            2 * np.pi * data_frame[self.date_feature_name].dt.hour / 24)
        data_frame[column_name + '_cos'] = np.cos(
            2 * np.pi * data_frame[self.date_feature_name].dt.hour / 24)
        return data_frame

    def _minute(self, data_frame, column_name="minute"):
        # retain cyclic nature of time
        data_frame[column_name + '_sin'] = np.sin(
            2 * np.pi * data_frame[self.date_feature_name].dt.minute / 60)
        data_frame[column_name + '_cos'] = np.cos(
            2 * np.pi * data_frame[self.date_feature_name].dt.minute / 60)
        return data_frame
