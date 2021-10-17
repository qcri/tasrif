"""
Corr operator
"""
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import GroupbyCompatibleValidatorMixin


class CorrOperator(GroupbyCompatibleValidatorMixin, PandasOperator):
    """

      Corr operator

      Examples
      --------

      >>> import pandas as pd
      >>> from tasrif.processing_pipeline.pandas import CorrOperator
      >>> df = pd.DataFrame([
      ...     [1, 1, 3],
      ...     [1, 1, 5],
      ...     [1, 2, 3],
      ...     [2, 1, 10],
      ...     [2, 1, 0]],
      ...     columns=['logId', 'sleep_level', 'awake_count'])
      >>>
      >>> df = df.set_index('logId')
      >>> op = CorrOperator()
      >>> df1 = op.process(df)
      >>> df1[0]
      sleep_level   awake_count
      sleep_level   1.000000  -0.181237
      awake_count   -0.181237   1.000000

    """
    def __init__(self, **kwargs):
        """Creates a new instance of CorrOperator

        Args:
            kwargs:
                Arguments to pandas pd.corr function

        """
        super().__init__(kwargs)
        self.kwargs = kwargs

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
            data_frame = data_frame.corr(**self.kwargs)
            processed.append(data_frame)

        return processed
