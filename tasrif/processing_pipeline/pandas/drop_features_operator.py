"""
Remove missing values from one or more dataframes.
"""
from tasrif.processing_pipeline import ProcessingOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin


class DropFeaturesOperator(InputsAreDataFramesValidatorMixin, ProcessingOperator):
    """

      Examples
      --------

      >>> import pandas as pd
      >>> import numpy as np
      >>>
      >>> from tasrif.processing_pipeline import DropFeaturesOperator
      >>>
      >>> df0 = pd.DataFrame([['Tom', 10], ['Alfred', 15], ['Alfred', 18], ['Juli', 14]], columns=['name', 'score'])
      >>> df1 = pd.DataFrame({"name": ['Alfred', 'juli', 'Tom', 'Ali'],
      ...                    "height": [np.nan, 155, 159, 165],
      ...                    "born": [pd.NaT, pd.Timestamp("2010-04-25"), pd.NaT,
      ...                             pd.NaT]})
      >>>
      >>> operator = DropFeaturesOperator(feature_names=['name'])
      >>> df0, df1 = operator.process(df0, df1)
      >>>
      >>> print(df0)
      >>> print(df1)
           name  score
      0     Tom     10
      1  Alfred     15
      2  Alfred     18
      3    Juli     14
           name  height       born
      0  Alfred     NaN        NaT
      1    juli   155.0 2010-04-25
      2     Tom   159.0        NaT
      3     Ali   165.0        NaT


    """

    def __init__(self, feature_names: list):
        """
        Initializes the operator

        Args:
            feature_names:
              features (columns) to drop from each dataframe

        """
        super().__init__()
        self.feature_names = feature_names

    def __str__(self):
        return self.__class__.__name__

    def _process(self, *data_frames):
        """Process the passed data using the processing configuration specified
        in the constructor

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            data_frames
                Processed data frames

        Raises:
            ValueError: Occurs when one of the objects in feature_names is not a column within
                *data_frames

        """

        processed = []
        for dataframe in data_frames:
            for col in self.feature_names:
                if col not in dataframe.columns:
                    raise ValueError(str(col) + ' not in columns')

            dataframe = dataframe.drop(self.feature_names, axis=1)
            processed.append(dataframe)

        return tuple(processed)
