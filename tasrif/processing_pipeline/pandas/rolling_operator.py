"""
Rolling Operator
"""
from tasrif.processing_pipeline import ProcessingOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin


class RollingOperator(InputsAreDataFramesValidatorMixin, ProcessingOperator):
    """

    Examples
    --------

    >>> import pandas as pd
    >>> import numpy as np
    >>> from tasrif.processing_pipeline.pandas import RollingOperator
    >>>
    >>>
    >>> df = pd.DataFrame({'B': [0, 1, 2, 3, 4]})
    >>>
    >>> op = RollingOperator(2)
    >>> op.process(df)[0].sum()
    .   B
    0   NaN
    1   1.0
    2   3.0
    3   5.0
    4   7.0

    """
    def __init__(self, winsize, selector=None, **kwargs):
        """Creates a new instance of RollingOperator

        Args:
            winsize (int):
                offset or BaseIndexer subclass
            selector:
                select the columns of a groupby object
            **kwargs:
              Arguments to pandas pd.groupby function

        """

        self.winsize = winsize
        self.selector = selector
        self.kwargs = kwargs
        super().__init__()

    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
                Variable number of pandas dataframes to be processed

        Returns:
            pd.DataFrame -or- list[pd.DataFrame]
                Processed dataframe(s) resulting from applying the operator

        """

        processed = []
        for data_frame in data_frames:
            if self.selector:
                data_frame = data_frame.rolling(self.winsize,
                                                **self.kwargs)[self.selector]
            else:
                data_frame = data_frame.rolling(self.winsize, **self.kwargs)
            processed.append(data_frame)

        return processed
