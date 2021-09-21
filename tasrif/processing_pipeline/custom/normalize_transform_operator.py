"""
Operator to normalize the integer/float values in a dataframe
"""
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import MaxAbsScaler
from sklearn.preprocessing import RobustScaler
from tasrif.processing_pipeline import ProcessingOperator


class NormalizeTransformOperator(ProcessingOperator):
    """

    Normalizes the values for the supplied feature_names using the supplied model.
    This operator works on a 2D data frames where the
    feature_names represent the features. The returned data frame contains normalized
    values in the specified feature_names.
    This will be used when splitting the dataset into training and testing set and then using the model generated
    from the testing set on the testing set.

    Examples
    --------

    >>> import pandas as pd
    >>> from sklearn.model_selection import train_test_split
    >>> from tasrif.processing_pipeline.custom import NormalizeOperator
    >>> from tasrif.processing_pipeline.custom import NormalizeTransformOperator

    >>> df = pd.DataFrame([
        [1, "2020-05-01 00:00:00", 10],
        [1, "2020-05-01 01:00:00", 15],
        [1, "2020-05-01 03:00:00", 23],
        [2, "2020-05-02 00:00:00", 17],
        [2, "2020-05-02 01:00:00", 11]],
        columns=['logId', 'timestamp', 'sleep_level'])

    >>> X_train, X_test, y_train, y_test = train_test_split(df['timestamp'], df['sleep_level'], test_size=0.4)

    >>> op1 = NormalizeOperator('all', 'minmax', {'feature_range': (0, 2)})

    >>> output1 = op1.process(y_train.to_frame())

    >>> print(output1)

    [(array([[2.        ],
       [1.33333333],
       [0.        ]]), MinMaxScaler(feature_range=(0, 2)))]

    >>> processed_train_y = output1[0][0]
    >>> trained_model = output1[0][1]

    >>> op2 = NormalizeTranformOperator('all', trained_model)

    >>> output2 = op2.process(y_test.to_frame())

    >>> print(output2)

    [array([[ 4.        ],
       [-0.33333333]])]

    """

    def __init__(self, feature_names='all', model=None):
        """
        Creates a new instance of NormalizeTransformOperator

        Args:
            feature_names (list, str):
                feature_names in the given dataframe to normalize
            model (StandardScaler / MinMaxScaler / MaxAbsScaler / RobustScaler):
                Model with normalization method ('zscore', 'minmax', 'maxabs', 'robust')

        Raises:
            ValueError: parameter method unknown.

        """
        super().__init__()
        self.feature_names = feature_names

        if not model:
            raise ValueError(
                "Incorrect model specified for the NormalizeTransformOperator!")

        if isinstance(model, StandardScaler):
            self.method = 'zscore'
        elif isinstance(model, MinMaxScaler):
            self.method = 'zscore'
        elif isinstance(model, MaxAbsScaler):
            self.method = 'maxabs'
        elif isinstance(model, RobustScaler):
            self.method = 'robust'
        else:
            raise ValueError(
                "Incorrect model specified for the NormalizeTransformOperator!")

        self.model = model

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
                data_frame_feature_names = data_frame[self.feature_names]
            else:
                data_frame_feature_names = data_frame[data_frame.select_dtypes(
                    include=np.number).columns.tolist()]

            processed.append(self.model.transform(
                data_frame_feature_names))

        return processed
