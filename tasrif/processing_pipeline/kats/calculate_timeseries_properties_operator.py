"""
Operator to aggregate column features based on a column
"""

from kats.consts import TimeSeriesData
from kats.tsfeatures.tsfeatures import TsFeatures
from tasrif.processing_pipeline import ProcessingOperator

class CalculateTimeseriesPropertiesOperator(ProcessingOperator):
    """
    This method extracts timeseries features from passed dataframe object

    >>> import numpy as np
    >>> import pandas as pd
    >>>
    >>> from tasrif.processing_pipeline.custom import ExtractTimeseriesFeaturesOperator
    >>>
    >>>
    >>> dates = pd.date_range('2016-12-31', '2020-01-08', freq='D').to_series()
    >>> df = pd.DataFrame()
    >>> df["Date"] = dates
    >>> df['Steps'] = np.random.randint(1000,25000, size=len(df))
    >>> df['Calories'] = np.random.randint(1800,3000, size=len(df))
    >>> df
    Date    Steps   Calories
    2016-12-31  2016-12-31  14648   2926
    2017-01-01  2017-01-01  9320    2190
    2017-01-02  2017-01-02  2798    2521
    2017-01-03  2017-01-03  11050   2330
    2017-01-04  2017-01-04  6536    2172
    ...     ...     ...     ...
    2020-01-04  2020-01-04  22739   2365
    2020-01-05  2020-01-05  4845    1849
    2020-01-06  2020-01-06  1143    2420
    2020-01-07  2020-01-07  5577    2821
    2020-01-08  2020-01-08  10435   1830

    >>> operator = ExtractTimeseriesFeaturesOperator(date_feature_name="Date", value_column='Steps')
    >>> features = operator.process(df)[0]
    >>> features
    {'length': 1104,
     'mean': 13024.617753623188,
     'var': 49311921.9535254,
     'entropy': 0.9344372604411008,
     'lumpiness': 98331798003649.39,
     'stability': 2856199.257016417,
     'flat_spots': 1,
     'hurst': 0.008804860201927526,
     'std1st_der': 4942.8445155096715,
     'crossing_points': 559,
     'binarize_mean': 0.5,
     'unitroot_kpss': 0.040225392515162994,
     'heterogeneity': 12.514530846049983,
     'histogram_mode': 1008.0,
     'linearity': 0.0010899360162310767,
     'trend_strength': 0.254325167656127,
     'seasonality_strength': 0.3490641758024736,
     'spikiness': 757835496.8906168,
     'peak': 5,
     'trough': 2,
     'level_shift_idx': 526,
     'level_shift_size': 1180.800000000001,
     'y_acf1': 0.04351456010311684,
     'y_acf5': 0.004305213630472914,
     'diff1y_acf1': -0.4827096027299026,
     'diff1y_acf5': 0.23486910790170704,
     'diff2y_acf1': -0.657077984628258,
     'diff2y_acf5': 0.45760076640469927,
     'y_pacf5': 0.004035237116844455,
     'diff1y_pacf5': 0.46635744288898123,
     'diff2y_pacf5': 1.0429819204003172,
     'seas_acf1': 0.0074998374601003975,
     'seas_pacf1': 0.010035485775513319,
     'firstmin_ac': 3,
     'firstzero_ac': 6,
     'holt_alpha': 0.21714285714285714,
     'holt_beta': 0.09771428571428571,
     'hw_alpha': 0.040357142857142855,
     'hw_beta': 0.024214285714285716,
     'hw_gamma': 0.03427295918367347}

    """
    def __init__(self, date_feature_name="time", value_column='value', method='kats', **kwargs):
        """Creates a new instance of ExtractTimeseriesFeaturesOperator

        Args:
            date_feature_name : str
                Name of the datetime column
            value_column : str
                Name of the column that contains values per date
            method : str
                Name of feature extractor method
            **kwargs : None or List[str]; list of feature/feature group name(s)
                key word arguments passed to method's parameters
        """
        super().__init__()
        self.date_feature_name = date_feature_name
        self.value_column = value_column
        self.method = method
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

            if self.method == 'kats':
                # convert to TimeSeriesData object
                timeseries_data = data_frame[[self.date_feature_name, self.value_column]]
                timeseries_data = TimeSeriesData(timeseries_data, time_col_name=self.date_feature_name)

                # calculate the TsFeatures
                features = TsFeatures(**self.kwargs).transform(timeseries_data)
                processed.append(features)

        return processed
