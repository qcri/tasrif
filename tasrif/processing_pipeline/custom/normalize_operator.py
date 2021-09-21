"""
Operator to normalize the integer/float values in a dataframe
"""
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import MaxAbsScaler
from sklearn.preprocessing import RobustScaler
from tasrif.processing_pipeline import ProcessingOperator


class NormalizeOperator(ProcessingOperator):
    """

    Normalizes the values for the supplied feature_names using the specified algorithm.
    This operator works on a 2D data frames where the
    feature_names represent the features. The returned data frame contains normalized values
    in the specified feature_names

    Examples
    --------

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.custom import NormalizeOperator
    >>> df = pd.DataFrame([
    >>>     [1, "2020-05-01 00:00:00", 1],
    >>>     [1, "2020-05-01 01:00:00", 1],
    >>>     [1, "2020-05-01 03:00:00", 2],
    >>>     [2, "2020-05-02 00:00:00", 1],
    >>>     [2, "2020-05-02 01:00:00", 1]],
    >>>     columns=['logId', 'timestamp', 'sleep_level'])
    >>>
    >>> df['timestamp'] = pd.to_datetime(df['timestamp'])
    >>> df = df.set_index('timestamp')
    >>> op = NormalizeOperator('all', 'minmax', {'min': 0, 'max': 2})
    >>> op.process(df)
    [            sleep_level
    timestamp
    2020-05-01     1.333333
    2020-05-02     1.000000]

    """
    def __init__(self,
                 feature_names='all',
                 method='zscore',
                 normalization_parameters=None):
        """
        Creates a new instance of NormalizeOperator

        Args:
            feature_names (list, str):
                feature_names in the given dataframe to normalize
            method (str):
                The normalization method ('zscore', 'minmax', 'maxabs', 'robust') to be used
            normalization_parameters (dict):
                Dictionary containing parameters for a specific normalization.::
                Default normalization parameters are as follows:
                    - If method is ``zscore``
                        ``normalization_params = {'with_mean': True, 'with_std': True}``
                    - If method is ``minmax``
                        ``normalization_params = {'feature_range': (0,1)}``
                    - If method is ``maxabs``
                        ``normalization_params = {}``
                    - If method is ``robust``
                        ``normalization_params = {'with_scaling': True, 'with_centering': True,
                        'quantile_range':(25.0, 75.0), 'unit_variance': False}``

        Raises:
            ValueError: parameter method unknown.

        """
        super().__init__()
        self.feature_names = feature_names
        if not normalization_parameters:
            normalization_parameters = {}

        if method in ['zscore', 'minmax', 'maxabs', 'robust']:
            if method == 'zscore':
                self.scaler = StandardScaler(**normalization_parameters)
            elif method == 'minmax':
                self.scaler = MinMaxScaler(**normalization_parameters)
            elif method == 'maxabs':
                self.scaler = MaxAbsScaler(**normalization_parameters)
            else:
                self.scaler = RobustScaler(**normalization_parameters)
        else:
            raise ValueError(
                "Incorrect method specified for the NormalizationOperator!")

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

            data_frame_feature_names = data_frame
            if isinstance(self.feature_names, list):
                data_frame = data_frame[self.feature_names]
            else:
                data_frame_feature_names = data_frame[data_frame.select_dtypes(
                    include=np.number).columns.tolist()]

            processed.append(
                (self.scaler.fit_transform(data_frame_feature_names),
                 self.scaler.fit(data_frame_feature_names)))

        return processed
