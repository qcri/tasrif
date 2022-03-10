"""Module that defines the PandasOperator class
"""

import warnings
from tasrif.processing_pipeline.processing_operator import ProcessingOperator


class PandasOperator(ProcessingOperator):
    """Interface specification of a pandas operator"""

    def __init__(self, kwargs):

        super().__init__(observers=kwargs.get("observers", []))
        if "observers" in kwargs:
            del kwargs["observers"]


    def _validate(self, *data_frames):
        """
        Validation hook that is run before any processing happens. Checks if
        any data_frame is empty to raise a warning

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Raises:
            Warning: If any input is empty. # noqa: DAR402
        """
        for data_frame in data_frames:
            if data_frame.empty:
                warnings.warn('One or more inputs are empty.')
