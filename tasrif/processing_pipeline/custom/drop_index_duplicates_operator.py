"""
Remove duplicate values from one or more dataframes.
"""
from tasrif.processing_pipeline import ProcessingOperator


class DropIndexDuplicatesOperator(ProcessingOperator):
    """
    Remove duplicate indices from one or more dataframes.

    Examples
    --------

    >>> import pandas as pd
    >>> import numpy as np
    >>>
    >>> from tasrif.processing_pipeline.custom import DropDuplicatesOperator
    >>>
    >>> idx = pd.Index(['1', '2', '2', '3'])
    >>> df = pd.DataFrame([['tom', 10], ['Alfred', 15], ['Alfred', 18],
    ... ['juli', 14]], columns=['name', 'age'], index=idx)
    >>>
    >>> operator = DropIndexDuplicatesOperator(keep='first')
    >>> df = operator.process(df)[0]
    >>>
    >>> print(df)
         name  age
    1     tom   10
    2  Alfred   15
    3    juli   14


    """

    def __init__(self, keep="first"):
        """
        Initializes the operator

        Args:
            keep ('first', 'last', False): The value or values in a set of duplicates to mark as missing.

        """
        super().__init__()
        self.keep = keep

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
            dataframe = dataframe[~dataframe.index.duplicated(self.keep)]
            processed.append(dataframe)

        return tuple(processed)
