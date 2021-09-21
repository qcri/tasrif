"""
Merge multiple dataframes into a single one.
"""
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin


class MergeOperator(InputsAreDataFramesValidatorMixin, PandasOperator):
    """Merge different datasets based on Pandas merge method.

    Examples
    --------
    >>> df1 = pd.DataFrame({'id': [1, 2, 3], 'colors': ['red', 'white', 'blue']})
    >>> df2 = pd.DataFrame({'id': [1, 2, 3], 'cities': ['Doha', 'Vienna', 'Belo Horizonte']})
    >>> merged = MergeOperator().process(df1, df2)
    >>> merged
    id    colors  cities
    1     red     Doha
    2     white   Vienna
    3     blue    Belo Horizonte
    """

    def __init__(self, **kwargs):
        """Merge different datasets on a common feature defined by ``on``.

        Args:
            **kwargs:
              key word arguments passed to pandas DataFrame.merge method

        """
        super().__init__(kwargs)
        self.kwargs = kwargs

    def _process(self, *data_frames):
        """Merge multiple datasets on a common feature defined on the constructor method.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            data_frame (pd.DataFrame):
                One processed data frame resulting on the merge of several input data frames.
        """
        if len(data_frames) < 2:
            return data_frames

        # Gets one single
        data_frame = data_frames[0]
        for dataframe in data_frames[1:]:
            data_frame = data_frame.merge(dataframe, **self.kwargs)

        return data_frame
