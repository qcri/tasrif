"""
Operator to aggregate column features based on a column
"""
from ummalqura.hijri_date import HijriDate

from tasrif.processing_pipeline import ProcessingOperator


class CategorizeTimeOperator(ProcessingOperator):
    """

      Given a 2D dataframe representing a timeseries where each row represents a time event, this operator
      will add a new feature(s) that represent a categorization of the date. The categorization specification is
      provided in the constructor.

      Examples
      --------

        >>> import numpy as np
        >>> import pandas as pd
        >>>
        >>> from tasrif.processing_pipeline.custom import CategorizeTimeOperator
        >>>
        >>>
        >>> dates = pd.date_range('2016-12-31', '2017-01-08', freq='D').to_series()
        >>> df = pd.DataFrame()
        >>> df["Date"] = dates
        >>> df['Steps'] = np.random.randint(1000,25000, size=len(df))
        >>> df['Calories'] = np.random.randint(1800,3000, size=len(df))
        >>>
        >>> df
                    Date	Steps	Calories
        2016-12-31	2016-12-31	5145	2486
        2017-01-01	2017-01-01	5018	2344
        2017-01-02	2017-01-02	11010	2426
        2017-01-03	2017-01-03	9304	2903
        2017-01-04	2017-01-04	13490	2283
        2017-01-05	2017-01-05	14511	1976
        2017-01-06	2017-01-06	18697	2213
        2017-01-07	2017-01-07	19204	2185
        2017-01-08	2017-01-08	4470	2333

        >>> df5 = df.copy()
        >>> operator = CategorizeTimeOperator(date_feature_name="Date",
        >>>    category_definition=[
        >>>         {"day": "weekday", "values": [1, 1, 1, 1, 0, 0, 1]},
        >>>         {"month": "in_may", "values": [0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]}])
        >>> operator.process(df5)[0]
                    Date	Steps	Calories	weekday	in_may
        2016-12-31	2016-12-31	5145	2486	0	0
        2017-01-01	2017-01-01	5018	2344	1	0
        2017-01-02	2017-01-02	11010	2426	1	0
        2017-01-03	2017-01-03	9304	2903	1	0
        2017-01-04	2017-01-04	13490	2283	1	0
        2017-01-05	2017-01-05	14511	1976	1	0
        2017-01-06	2017-01-06	18697	2213	0	0
        2017-01-07	2017-01-07	19204	2185	0	0
        2017-01-08	2017-01-08	4470	2333	1	0

    """
    def __init__(self,
                 date_feature_name="date",
                 category_definition="day"):
        """Creates a new instance of CategorizeTimeOperator

        Args:
            date_feature_name (str):
                Name of the feature to identify related timestamp series
            category_definition (str, list, dict):
                Value is one of "day", "month" or "hijri_month" to categorize based on
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

                Array of dictionary with mapping if the default categories are to mapped to customized categories.py
                For example to categorize based on weekday::

                    [
                        { "days": "weekday", "values": [1, 1, 1, 1, 0, 0, 1]}
                        { "month": "winter", "values": [1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1] }
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
                if k != "values":
                    function = getattr(self, f"_{k}")
                    data_frame = function(data_frame, val, category.get("values"))
                    break
        return data_frame


    def _day(self, data_frame, column_name="day", values=None):

        data_frame[column_name] = data_frame[self.date_feature_name].dt.dayofweek
        if values:
            data_frame[column_name] = data_frame[column_name].apply(lambda x: values[x])
        return data_frame

    def _month(self, data_frame, column_name="month", values=None):

        data_frame[column_name] = data_frame[self.date_feature_name].dt.month
        if values:
            data_frame[column_name] = data_frame[column_name].apply(lambda x: values[x - 1])
        return data_frame


    def _hijri_month(self, data_frame, column_name="hijri_month", values=None):

        data_frame[column_name] = data_frame[self.date_feature_name].apply(HijriDate.hijri_month_from_date)
        if values:
            data_frame[column_name] = data_frame[column_name].apply(lambda x: values[x])
        return data_frame
