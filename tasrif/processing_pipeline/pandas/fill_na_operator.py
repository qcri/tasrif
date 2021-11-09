"""
Fill NaN values for one or more dataframes
"""
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import GroupbyCompatibleValidatorMixin


class FillNAOperator(GroupbyCompatibleValidatorMixin, PandasOperator):
    """

    Examples
    --------

    >>> import pandas as pd
    >>> import numpy as np
    >>>
    >>> from tasrif.processing_pipeline.pandas import FillNAOperator
    >>>
    >>> df = pd.DataFrame({"name": ['Alfred', 'juli', 'Tom', 'Ali'],
    ...                   "height": [np.nan, 155, 159, 165],
    ...                   "born": [pd.NaT, pd.Timestamp("2010-04-25"), pd.NaT,
    >>>
    >>>
    >>> operator = FillNAOperator(axis=0, value='laptop')
    >>> df = operator.process(df)[0]
    >>> df
    name    height  born
    0   Alfred  laptop  laptop
    1   juli    155     2010-04-25 00:00:00
    2   Tom     159     laptop
    3   Ali     165     laptop

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
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            data_frames
                Processed data frames
        """

        processed = []
        for dataframe in data_frames:
            dataframe = dataframe.fillna(**self.kwargs)
            processed.append(dataframe)

        return tuple(processed)
