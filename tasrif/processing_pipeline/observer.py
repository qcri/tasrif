"""Module that defines the Observer class
"""


class Observer:
    """Interface specification of a observer
    The constructor of a concrete operator will provide options to configure the
    operation. The processing is invoked via the process method and the data to be
    processed is passed to the process method.
    """

    def observe(self, *data_frames):
        """
        Observe the passed data using the processing configuration specified
        in the constructor

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be observed
        """
        