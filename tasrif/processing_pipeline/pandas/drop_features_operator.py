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
      >>> df0 = pd.DataFrame([['tom', 10], ['nick', 15], ['juli', 14]],
      >>>                     columns=['name', 'age'])
      >>> df1 = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
      >>>                   "toy": [np.nan, 'Batmobile', 'Bullwhip'],
      >>>                   "born": [pd.NaT, pd.Timestamp("1940-04-25"),
      >>>                            pd.NaT]})
      >>>
      >>> operator = DropFeaturesOperator(feature_names=['name'])
      >>> df0, df1 = operator.process(df0, df1)
      >>>
      >>> print(df0)
      >>> print(df1)
      .  name  age
      0   tom   10
      1  nick   15
      2  juli   14
      .      name        toy       born
      0    Alfred        NaN        NaT
      1    Batman  Batmobile 1940-04-25
      2  Catwoman   Bullwhip        NaT

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
