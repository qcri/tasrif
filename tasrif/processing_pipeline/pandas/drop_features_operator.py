"""
Remove missing values from one or more dataframes.
"""
from tasrif.processing_pipeline import ProcessingOperator


class DropFeaturesOperator(ProcessingOperator):
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
      >>> operator = DropFeaturesOperator(drop_features=['name'])
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

    def __init__(self, drop_features: list):
        """
        Initializes the operator

        Args:
            drop_features:
              features (columns) to drop from each dataframe

        """
        self.drop_features = drop_features
        super().__init__()

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
            ValueError: Occurs when one of the objects in drop_features is not a column within
                *data_frames

        """

        processed = []
        for dataframe in data_frames:
            for col in self.drop_features:
                if col not in dataframe.columns:
                    raise ValueError(str(col) + ' not in columns')

            dataframe = dataframe.drop(self.drop_features, axis=1)
            processed.append(dataframe)

        return tuple(processed)
