"""Module that defines the Observer class
"""

class Observer:
    """Interface specification of a observer
    The observation is invoked via the observe method and the data to be
    observed is passed to the observe method.
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

        Args:
            operator (ProcessingOperator):
                Processing operator which is observed
            *data_frames (list of pd.DataFrame):
                Variable number of pandas dataframes to be observed
        """
        self._observe(operator, *data_frames)
        