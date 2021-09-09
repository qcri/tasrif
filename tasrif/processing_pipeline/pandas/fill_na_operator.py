"""
Fill NaN values for one or more dataframes
"""
from tasrif.processing_pipeline import ProcessingOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin


class FillNAOperator(InputsAreDataFramesValidatorMixin, ProcessingOperator):
    """

    Examples
    --------

    >>> import pandas as pd
    >>> import numpy as np
    >>>
    >>> from tasrif.processing_pipeline.pandas import FillNAOperator
    >>>
    >>> df = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
    ...                    "toy": [np.nan, 'Batmobile', 'Bullwhip'],
    ...                    "born": [pd.Timestamp("1940-04-25"), pd.Timestamp("1940-04-25"),
    ...                             pd.Timestamp("1940-04-25")]})
    >>>
    >>> operator = FillNAOperator(axis=0, value='laptop')
    >>> df = operator.process(df)[0]
    >>> df
        name    toy     born
    0   Alfred  laptop  1940-04-25
    1   Batman  Batmobile   1940-04-25
    2   Catwoman    Bullwhip    1940-04-25

    """

    def __init__(self, **kwargs):
        """
        Initializes the operator

        Args:
            **kwargs:
              key word arguments passed to pandas DataFrame.dropna method
        """
        self.kwargs = kwargs
        super().__init__()

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
