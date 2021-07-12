"""
Sort multiple dataframes based on some keys
"""
from tasrif.processing_pipeline import ProcessingOperator


class SortOperator(ProcessingOperator):
    """Sort datasets based on Pandas `sort_values` method.

    Examples
    ========
    >>> df1 = pd.DataFrame({'id': [1, 2, 3], 'colors': ['red', 'white', 'blue'], "importance": [1, 3, 2]})
    >>> df2 = pd.DataFrame({'id': [1, 2, 3], 'cities': ['Doha', 'Vienna', 'Belo Horizonte'], "importance": [3, 2, 1]})
    >>> sorted_dfs = SortOperator().process(df1, df2)
    >>> sorted_dfs[0]
    id	colors	importance
    1	    red	    1
    3	    blue	2
    2	    white	3

    >>> sorted_dfs[1]
    id	cities	        importance
    3	    Belo Horizonte	1
    2	    Vienna	        2
    1	    Doha	        3
    """

    def __init__(self, **kwargs):
        """Sort datasets using the Pandas function `sort_values`.

        Parameters
        ----------
        data_frames:
          Variable number of pandas dataframes to be processed

        \\*\\*kwargs:
          key word arguments passed to pandas DataFrame.sort_values method


        """
        self.kwargs = kwargs
        super().__init__()

    def process(self, *data_frames):
        """Sort multiple datasets using the Pandas function `sort_values`.

        Returns
        -------
        data_frames
            Resulting dataframes after sorting.
        """
        # Gets one single
        processed = []
        for data_frame in data_frames:
            data_frame = data_frame.sort_values(**self.kwargs)
            processed.append(data_frame)
        return processed
