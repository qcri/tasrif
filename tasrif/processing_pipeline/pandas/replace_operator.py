"""
Replaces values in multiple dataframes based on Pandas `replace` method.
"""
from tasrif.processing_pipeline import ProcessingOperator


class ReplaceOperator(ProcessingOperator):
    """Replaces a value by another on the datasets based on Pandas `replace` method.

    Parameters
    ----------

    Returns
    -------

    Examples
    --------
    >>> df = pd.DataFrame({'id': [1, 2, 3], 'colors': ['red', 'white', 'blue'], "importance": [1, 3, 2]})
    >>> df = ReplaceOperator(to_replace="red", value="green").process(df)[0]
    >>> df
    id	colors	    importance
    1    green	    1
    3    blue	    2
    2    white	    3
    """

    def __init__(self, **kwargs):
        """Replaces values in the datasets using the Pandas function `replace`.

        Parameters
        ----------
        data_frames:
          Variable number of pandas dataframes to be processed

        \\*\\*kwargs:
          key word arguments passed to pandas `DataFrame.replace` method


        """
        self.kwargs = kwargs
        super().__init__()

    def process(self, *data_frames):
        """Replaces values in multiple datasets using the Pandas function `replace`.

        Returns
        -------
        data_frames
            Resulting dataframes after applying the replace function.
        """
        # Gets one single
        processed = []
        for data_frame in data_frames:
            data_frame = data_frame.replace(**self.kwargs)
            processed.append(data_frame)
        return processed
