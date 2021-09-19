# pylint: disable=missing-module-docstring
import pandas as pd
from tasrif.processing_pipeline import PandasOperator

class ReadCsvOperator(PandasOperator):
    """
    Operator that takes a path to a csv file and reads it into a dataframe.
    Uses the ``pandas.read_csv`` method underneath.

    Parameters
    ----------
    *args:
        Arguments passed to ``pandas.read_csv``.

    **kwargs:
        Keyword arguments passed to ``pandas.read_csv``.

    Examples
    --------
    >>> from tasrif.processing_pipeline.pandas import ReadCsvOperator
    >>> operator = ReadCsvOperator(file_path='example.csv')
    >>> dfs = operator.process()
    >>> dfs[0]
       record        date  count
    0  8FGH1A  05-03-2021   6000
    1  CB12DG  10-03-2021   7800
    2  AD23F1  15-03-2021   9500
    """

    def __init__(self, *args, **kwargs):
        self.args = args
        super().__init__(kwargs)
        self.kwargs = kwargs

    def _process(self, *data_frames):
        csv_dataframe = pd.read_csv(*self.args, **self.kwargs)
        return [csv_dataframe]
