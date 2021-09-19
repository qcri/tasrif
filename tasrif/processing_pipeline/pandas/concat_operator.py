"""
Concatenate multiple dataframes into a single one.
"""
import pandas as pd
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin

class ConcatOperator(InputsAreDataFramesValidatorMixin, PandasOperator):
    """Concatenate different datasets based on Pandas concat method.

    Examples
    --------

    >>> import pandas as pd
    >>>
    >>> from tasrif.processing_pipeline.pandas import ConcatOperator
    >>>
    >>> # Full
    >>> df1 = pd.DataFrame({'id': [1, 2, 3], 'cities': ['Rome', 'Barcelona', 'Stockholm']})
    >>> df2 = pd.DataFrame({'id': [4, 5, 6], 'cities': ['Doha', 'Vienna', 'Belo Horizonte']})
    >>>
    >>> concat = ConcatOperator().process(df1, df2)
    >>> concat
        id  cities
    0   1   Rome
    1   2   Barcelona
    2   3   Stockholm
    0   4   Doha
    1   5   Vienna
    2   6   Belo Horizonte

    """

    def __init__(self, **kwargs):
        """Merge different datasets on a common feature defined by ``on``.

        Args:
            **kwargs:
              key word arguments passed to pandas concat method


        """
        super().__init__(kwargs)
        self.kwargs = kwargs

    def _process(self, *data_frames):
        """Concatenate multiple datasets.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            data_frame
                Concatenated dataframe based on the input data_frames.
        """
        data_frame = pd.concat(list(data_frames), **self.kwargs)
        return [data_frame]
