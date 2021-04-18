"""
Mean operator
"""
from tasrif.processing_pipeline import ProcessingOperator

class MeanOperator(ProcessingOperator):
    """

      Mean operator

      Examples
      --------
      >>> import pandas as pd
      >>> from tasrif.processing_pipeline.pandas import MeanOperator
      >>> df = pd.DataFrame([
      >>>     [1, 1, 3],
      >>>     [1, 1, 5], 
      >>>     [1, 2, 3], 
      >>>     [2, 1, 10],
      >>>     [2, 1, 0]],
      >>>     columns=['logId', 'sleep_level', 'awake_count'])
      >>> 
      >>> df = df.set_index('logId')
      >>> op = MeanOperator()
      >>> df1 = op.process(df)
      >>> df1[0]

      sleep_level     6
      awake_count    21
      dtype: int64
      
    """

    def __init__(self, **kwargs):
        """Creates a new instance of SumOperator

        Parameters
        ----------
        kwargs: Arguments to pandas pd.mean function

        """
        self.kwargs = kwargs


    def process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Returns
        -------
        pd.DataFrame -or- list[pd.DataFrame]
            Processed dataframe(s) resulting from applying the operator
        """


        processed = []
        for data_frame in data_frames:
            data_frame = data_frame.mean(**self.kwargs)
            processed.append(data_frame)

        return processed
