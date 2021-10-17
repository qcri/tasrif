"""
Sum operator

"""
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import GroupbyCompatibleValidatorMixin

class SumOperator(GroupbyCompatibleValidatorMixin, PandasOperator):
    """

    Sum operator

    Examples
    --------

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.pandas import SumOperator
    >>> df = pd.DataFrame([
    >>>     [1, 1, 3],
    >>>     [1, 1, 5],
    >>>     [1, 2, 3],
    >>>     [2, 1, 10],
    >>>     [2, 1, 0]],
    >>>     columns=['logId', 'sleep_level', 'awake_count'])
    >>>
    >>> df = df.set_index('logId')
    >>> op = SumOperator()
    >>> df1 = op.process(df)
    >>> df1[0]
    sleep_level     6
    awake_count    21
    dtype: int64

    """

    def __init__(self, **kwargs):
        """Creates a new instance of SumOperator

        Args:
            **kwargs:
                Arguments to pandas pd.sum function

        """
        super().__init__(kwargs)
        self.kwargs = kwargs


    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            data_frames (list of pd.DataFrame):
                Variable number of pandas dataframes to be processed

        Returns:
            data_frames (list of pd.DataFrame):
                Resulting dataframes after applying the replace function.

        """

        processed = []
        for data_frame in data_frames:
            data_frame = data_frame.sum(**self.kwargs)
            processed.append(data_frame)

        return processed
