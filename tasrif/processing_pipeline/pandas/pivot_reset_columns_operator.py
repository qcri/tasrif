"""
Pivots a dataframe and realigns its columns to remove any multi-indices.
"""
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin

class PivotResetColumnsOperator(InputsAreDataFramesValidatorMixin, PandasOperator):
    """
    Pivots a dataframe and realigns its columns to remove any multi-indices.
    """
    def __init__(self, level, **kwargs):
        """
        Creates a new instance of PivotResetColumnsOperator

        Examples
        --------
        >>> import pandas as pd
        >>> from tasrif.processing_pipeline.pandas import PivotResetColumnsOperator
        >>>
        >>> df = pd.DataFrame([
        ...     [1, "2020-05-01 00:00:00", 1],
        ...     [1, "2020-05-01 01:00:00", 1],
        ...     [1, "2020-05-01 03:00:00", 2],
        ...     [2, "2020-05-02 00:00:00", 1],
        ...     [1, "2020-05-02 00:00:00", 2],
        ...     [2, "2020-05-02 01:00:00", 1]],
        ...     columns=['logId', 'timestamp', 'sleep_level'])
        >>>
        >>> df['timestamp'] = pd.to_datetime(df['timestamp'])
        >>>
        >>> op = PivotResetColumnsOperator(level=0, index='timestamp', columns='logId', values='sleep_level')
        >>> op.process(df)[0]
        .   timestamp	            1	    2
        0	2020-05-01 00:00:00	    1.0	    NaN
        1	2020-05-01 01:00:00	    1.0	    NaN
        2	2020-05-01 03:00:00	    2.0	    NaN
        3	2020-05-02 00:00:00	    2.0	    1.0
        4	2020-05-02 01:00:00	    NaN	    1.0

        Args:
            level (int or str):
                Either the integer position or the name of the level to reset the columns to.
            **kwargs: Arguments to pandas pivot function
        """
        self.level = level
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
            data_frame = data_frame.pivot(**self.kwargs)
            data_frame.columns = data_frame.columns.get_level_values(self.level)
            data_frame.columns.name = None
            data_frame.reset_index(inplace=True)
            processed.append(data_frame)

        return processed
