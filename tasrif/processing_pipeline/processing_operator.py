"""Module that defines the ProcessingOperator class
"""


class ProcessingOperator:
    """Interface specification of a processing operator
    The constructor of a concrete operator will provide options to configure the
    operation. The processing is invoked via the process method and the data to be
    processed is passed to the process method.
    """

    observers = []

    def __init__(self, observers=None):
        if observers:
            self.set_observers(observers)

    def set_observers(self, observers):
        """
        Function to store the observers for the given operator.

        Args:
            observers (list of Observer):
              Observer objects that observe the operator
        """
        if observers and not self.observers:
            self.observers = observers

    def _observe(self, *data_frames):
        """
        Function that runs the observe method for each observer for the given operator

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be observed
        """
        for observer in self.observers:
            observer.observe(*data_frames)

    def _validate(self, *data_frames):
        """
        Validation hook that is run before any processing happens. This method should
        be overriden in the child class to perform validations on input dataframes.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Raises:
            ValidationError: If any validation fails. # noqa: DAR402
        """

    #pylint: disable=no-self-use
    def _process(self, *data_frames):
        """Process the passed data using the processing configuration specified
        in the constructor

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed
        """
        return data_frames

    def process(self, *data_frames):
        """
        Function that runs validation hooks and processes the input data_frames.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            Output of _process method
        """
        self._validate(*data_frames)
        result = self._process(*data_frames)
        self._observe(result)
        return result
