"""Module that defines the Logger class
"""
from tasrif.processing_pipeline.observers.functional_observer import FunctionalObserver

class GroupbyLogger(FunctionalObserver):
    """GroupbyLogger class to log a dataframe after grouping
    """

    def __init__(self, groupby_args, method=""):
        """
        The constructor of the GroupbyLogger class will provide options to configure the
        operation using keyword arguments. The logging is invoked via the observe method
        and the data to be logged is passed to the observe method.

        Args:
            groupby_args (String or list):
                Arguments to pandas pd.groupby function
            method (String):
                Logging method to log the dataframe
                Options: "head", "tail", "info", "first", "last"
        """
        self.groupby_args = groupby_args
        self._logging_methods = []
        if method:
            self._logging_methods = method.split(',')

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
            if self._logging_methods:
                for logging_method in self._logging_methods:
                    print(getattr(data_frame[0].groupby(self.groupby_args), logging_method)())

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
