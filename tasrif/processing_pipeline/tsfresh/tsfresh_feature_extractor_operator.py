"""
Operator to extract features from a dataframe
"""

from tsfresh import extract_features
from tasrif.processing_pipeline import ProcessingOperator


class TSFreshFeatureExtractorOperator(ProcessingOperator):
    """

    From a timeseries dataframe of participants, this function generates two dataframes:


      Examples
      --------

        >>> import numpy as np
        >>> import pandas as pd
        >>>
        >>> from tasrif.processing_pipeline.tsfresh import TSFreshFeatureExtractorOperator
        >>>
        >>> df = pd.DataFrame([
        ...     ["2020-02-16 15:15:00",98.86667,105.0,105.0,105.0,282.5,0],
        ...     ["2020-02-16 15:30:00",87.26667,94.0,94.0,94.0,275.0,0],
        ...     ["2020-02-16 15:45:00",94.53333,94.0,94.0,94.0,250.0,0],
        ...     ["2020-02-16 16:00:00",86.666664,95.0,95.0,95.0,235.0,0],
        ...     ["2020-02-16 16:15:00",86.86667,84.0,84.0,84.0,206.5,0],
        ...     ["2020-02-16 16:30:00",88.53333,101.0,101.0,101.0,191.0,0],
        ...     ["2020-02-16 16:45:00",76.066666,68.0,68.0,68.0,166.5,0],
        ...     ["2020-02-19 08:00:00",85.333336,100.0,100.0,100.0,108.0,1],
        ...     ["2020-02-19 08:15:00",86.933334,83.0,83.0,83.0,110.0,1],
        ...     ["2020-02-19 08:30:00",86.73333,84.0,84.0,84.0,116.0,1],
        ...     ["2020-02-19 08:45:00",93.066666,84.0,84.0,84.0,163.0,1],
        ...     ["2020-02-19 09:00:00",95.13333,98.0,98.0,98.0,183.5,1],
        ...     ["2020-02-19 09:15:00",89.2,110.0,110.0,110.0,177.0,1],
        ...     ["2020-02-19 09:30:00",85.933334,79.0,79.0,79.0,171.0,1],
        ...     ["2020-02-21 00:15:00",88.6,103.0,103.0,103.0,153.0,2],
        ...     ["2020-02-21 00:30:00",83.53333,85.0,85.0,85.0,139.0,2],
        ...     ["2020-02-21 00:45:00",86.2,84.0,84.0,84.0,131.0,2],
        ...     ["2020-02-21 01:00:00",85.933334,82.0,82.0,82.0,124.0,2],
        ...     ["2020-02-21 01:15:00",88.933334,89.0,89.0,89.0,120.0,2],
        ...     ["2020-02-21 01:30:00",78.53333,77.0,77.0,77.0,119.0,2],
        ...     ["2020-02-21 01:45:00",76.13333,79.0,79.0,79.0,116.0,2]],
        ...     columns=['dateTime','HeartRate','Calories','Steps','Distance','CGM','seq_id']
        ... )
        >>>
        >>> df['dateTime'] = pd.to_datetime(df['dateTime'])
        >>> df
        >>>
        >>> operator = TSFreshFeatureExtractorOperator(seq_id_col="seq_id", date_feature_name='dateTime',
        >>>                                            value_col='Steps')
        >>> features = operator.process(df)[0]
        >>> features[features.columns[2:4]]
        Steps__agg_linear_trend__attr_"slope"__chunk_len_5__f_agg_"max"
        Steps__agg_linear_trend__attr_"slope"__chunk_len_5__f_agg_"mean"
        0   -4.0    -9.9
        1   10.0    4.7
        2   -24.0   -10.6

    """

    class Defaults: #pylint: disable=too-few-public-methods
        """Default parameters used by the class."""

        TSFRESH_FEATURES = {'agg_linear_trend': [{'attr': 'slope', 'chunk_len': 50, 'f_agg': 'mean'},
                                                                {'attr': 'slope', 'chunk_len': 10, 'f_agg': 'var'},
                                                                {'attr': 'slope', 'chunk_len': 5, 'f_agg': 'max'},
                                                                {'attr': 'slope', 'chunk_len': 5, 'f_agg': 'mean'},
                                                                {'attr': 'rvalue', 'chunk_len': 5, 'f_agg': 'max'},
                                                                {'attr': 'slope', 'chunk_len': 50, 'f_agg': 'var'},
                                                                {'attr': 'rvalue', 'chunk_len': 5, 'f_agg': 'mean'},
                                                                {'attr': 'rvalue', 'chunk_len': 5, 'f_agg': 'var'},
                                                                {'attr': 'slope', 'chunk_len': 10, 'f_agg': 'mean'},
                                                                {'attr': 'intercept', 'chunk_len': 5, 'f_agg': 'mean'},
                                                                {'attr': 'slope', 'chunk_len': 50, 'f_agg': 'max'},
                                                                {'attr': 'slope', 'chunk_len': 5, 'f_agg': 'var'},
                                                                {'attr': 'rvalue', 'chunk_len': 10, 'f_agg': 'var'},
                                                                {'attr': 'slope', 'chunk_len': 10, 'f_agg': 'max'},
                                                                {'attr': 'intercept', 'chunk_len': 5, 'f_agg': 'var'},
                                                                {'attr': 'rvalue', 'chunk_len': 10, 'f_agg': 'max'},
                                                                {'attr': 'intercept', 'chunk_len': 5, 'f_agg': 'max'},
                                                                {'attr': 'rvalue', 'chunk_len': 10, 'f_agg': 'mean'},
                                                                {'attr': 'intercept', 'chunk_len': 10, 'f_agg': 'mean'},
                                                                {'attr': 'intercept', 'chunk_len': 10, 'f_agg': 'var'},
                                                                {'attr': 'intercept', 'chunk_len': 10, 'f_agg': 'max'},
                                                                {'attr': 'rvalue', 'chunk_len': 50, 'f_agg': 'max'}],
                                           'linear_trend': [{'attr': 'rvalue'},
                                                            {'attr': 'slope'},
                                                            {'attr': 'intercept'}],
                                           'index_mass_quantile': [{'q': 0.4},
                                                                   {'q': 0.7},
                                                                   {'q': 0.6},
                                                                   {'q': 0.8},
                                                                   {'q': 0.3}],
                                           'cwt_coefficients': [{'coeff': 3, 'w': 2, 'widths': (2, 5, 10, 20)},
                                                                {'coeff': 7, 'w': 2, 'widths': (2, 5, 10, 20)}],
                                           'last_location_of_maximum': None,
                                           'fft_coefficient': [{'attr': 'imag', 'coeff': 1},
                                                               {'attr': 'imag', 'coeff': 8}],
                                           'first_location_of_maximum': None,
                                           'energy_ratio_by_chunks': [{'num_segments': 10,
                                                                       'segment_focus': 9}]}


    def __init__(self,  # pylint: disable=too-many-arguments
                 seq_id_col="seq_id",
                 date_feature_name="time",
                 value_col="Steps",
                 features=Defaults.TSFRESH_FEATURES):
        """Creates a new instance of TSFreshFeatureExtractorOperator

        Args:
            seq_id_col (str):
                label column in the dataframe
            date_feature_name (str):
                time column in the dataframe
            value_col (str or list):
                column(s) to extract features from
            features (dict):
                list of features to be passed to tsfresh extract_features method.
                (see https://tsfresh.readthedocs.io/en/latest/api/tsfresh.feature_extraction.html) for
                more details

        """
        super().__init__()
        self.date_feature_name = date_feature_name
        self.seq_id_col = seq_id_col
        self.value_col = value_col
        self.features = features

    def process(self, *data_frames):
        """

        Processes the passed data frame as per the configuration define in the constructor.
        Currently uses custom parameters for the extraction of specific features.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            pd.DataFrame -or- list[pd.DataFrame]
                Processed dataframe(s) resulting from applying the operator

        Raises:
            ValueError: Occurs when one of self.value_col is not a column within the *data_frames

        """

        if isinstance(self.value_col, str):
            self.value_col = [self.value_col]

        for data_frame in data_frames:
            for column in self.value_col:
                if column not in data_frame.columns:
                    raise ValueError(str(column) + ' not in columns')

        kind_to_fc_parameters = {}
        for column in self.value_col:
            kind_to_fc_parameters[column] = self.features

        processed = []
        for data_frame in data_frames:
            tsfresh_features = extract_features(data_frame,
                                               column_id=self.seq_id_col,
                                               column_sort=self.date_feature_name,
                                               kind_to_fc_parameters=kind_to_fc_parameters)
            processed.append(tsfresh_features)

        return processed
