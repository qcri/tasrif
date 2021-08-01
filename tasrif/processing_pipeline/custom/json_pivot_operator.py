"""
Operator that returns an iterator of json data.
"""
import json
import pandas as pd

from tasrif.processing_pipeline import ProcessingOperator

class JsonPivotOperator(ProcessingOperator):
    """
    Operator that converts column with structured json data into dataframe multiple columns

    Example
    -------

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.custom import JsonPivotOperator

    >>> df = pd.DataFrame({'id': [1, 2, 3], 'data':["{\"calories\":1000, \"distance\":5, \"steps\":2}",
    ... "{\"calories\":2000, \"distance\":15, \"steps\":12}", "{\"calories\":1000, \"distance\":5, \"steps\":2}"]})

    >>> op = JsonPivotOperator(['data'])
    >>> op.process(df)

    >>> op

        id  calories  distance  steps
    0   1      1000         5      2
    1   2      2000        15     12
    2   3      1000         5      2

    """
    def __init__(self, columns):
        """Creates a new instance of JsonPivotOperator

        Args:
            columns (list of str):
              The columns which contains Json data
        """

        self.columns = columns

    def process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            list[pd.DataFrame]
              list of Dataframes
        """
        output = []
        for data_frame in data_frames:
            for column in self.columns:
                data_frame[column] = data_frame[column].apply(json.loads)
                temp_df = pd.DataFrame(data_frame[column].values.tolist())
                data_frame = pd.concat([data_frame.drop(column, axis=1), temp_df], axis=1)
        return output
