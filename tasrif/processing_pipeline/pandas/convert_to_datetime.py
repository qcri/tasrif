"""
Operator to convert a column feature from string to datetime
"""
import pandas as pd
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin


class ConvertToDatetimeOperator(InputsAreDataFramesValidatorMixin, PandasOperator):
    """

    Converts a set of (string) features to datetime using Pandas ``to_datetime``

    Examples
    --------

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator
    >>>
    >>> df0 = pd.DataFrame([[1, "2020-05-01 00:00:00", 1], [1, "2020-05-01 01:00:00", 1],
    >>>                     [1, "2020-05-01 03:00:00", 2], [2, "2020-05-02 00:00:00", 1],
    >>>                     [2, "2020-05-02 01:00:00", 1]],
    >>>                     columns=['logId', 'timestamp', 'sleep_level'])
    >>>
    >>> operator = ConvertToDatetime(feature_names=["timestamp"], utc=True)
    >>> df0 = operator.process(df0)
    >>>
    >>> print(df0)
    .   logId   timestamp   sleep_level
    0   1   2020-05-01 00:00:00+00:00   1
    1   1   2020-05-01 01:00:00+00:00   1
    2   1   2020-05-01 03:00:00+00:00   2
    3   2   2020-05-02 00:00:00+00:00   1
    4   2   2020-05-02 01:00:00+00:00   1

    """

    def __init__(self, feature_names, **kwargs):
        """Convert a set of columns features from string to datetime

        Args:
            feature_names (str):
                Name of the string columns that represent datetime objects
            **kwargs:
              key word arguments passed to pandas ``to_datetime`` method

        """
        self.feature_names = feature_names
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
        columns = self.feature_names.copy() if isinstance(self.feature_names, list) else [self.feature_names]

        processed = []
        for data_frame in data_frames:
            for col in columns:
                data_frame[col] = pd.to_datetime(data_frame[col], **self.kwargs)
            processed.append(data_frame)
        return processed
