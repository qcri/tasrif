"""Module that defines the FunctionalObserver class
"""
from tasrif.processing_pipeline.observer import Observer

class FunctionalObserver(Observer):
    """Interface specification of a observer
    The constructor of a concrete operator will provide options to configure the
    operation. The processing is invoked via the process method and the data to be
    processed is passed to the process method.
    """

    def _observe(self, operator, *data_frames):
        """
        Observe the passed data using the processing configuration specified
        in the constructor

        Args:
            operator (ProcessingOperator):
                Processing operator which is observed
            *data_frames (list of pd.DataFrame):
                Variable number of pandas dataframes to be observed
        """

    def observe(self, operator, *data_frames):
        """
        Function that performs checks on operator and data frame before observation
        This observation is only performed on non-infrastructure operators

        Args:
            operator (ProcessingOperator):
                Processing operator which is observed
            *data_frames (list of pd.DataFrame):
                Variable number of pandas dataframes to be observed
        """

        if operator.is_functional():
            self._observe(operator, *data_frames)
