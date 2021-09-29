"""Module that defines the Logger class
"""
from tasrif.processing_pipeline.observers.functional_observer import FunctionalObserver

class Logger(FunctionalObserver):
    """Logger class to log a dataframe in different ways
    """

    def __init__(self, method=""):
        """
        The constructor of the Logger class will provide options to configure the
        operation. The logging is invoked via the observe method and the data to be
        logged is passed to the observe method.

        Args:
            method (String):
                Logging method to log the dataframe
                Options: "head", "tail", "info", "first", "last"
        """
        self._logging_method = method

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
        for data_frame in data_frames:
            if self._logging_method == '':
                print(data_frame)
            elif self._logging_method == 'first':
                print(data_frame[0].groupby('id').first())
            elif self._logging_method == 'last':
                print(data_frame[0].groupby('id').last())
            else:
                print(getattr(data_frame[0], self._logging_method)())

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