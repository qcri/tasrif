"""Module that defines the ProcessingOperator class
"""


class ProcessingOperator:
    """Interface specification of a processing operator
    The constructor of a concrete operator will provide options to configure the
    operation. The processing is invoked via the process method and the data to be
    processed is passed to the process method.
    """

    def __init__(self, observers=None):
        """Base processing operator class

        Args:
            observers (list[Observer]):
                Python list of observers
        """
        self._observers = []
        if observers:
            self.set_observers(observers)

    def set_observers(self, observers):
        """
        Function to store the observers for the given operator.

        Args:
            observers (list of Observer):
              Observer objects that observe the operator
        """
        if observers and not self._observers:
            self._observers = observers

    def observe(self, *data_frames):
        """
        Function that runs the observe method for each observer for the given operator

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be observed

        Raises:
            RuntimeError: Occurs when super().__init__() is not called in the __init__() method of
                the ProcessingOperator
        """
        if not hasattr(self, '_observers'):
            raise RuntimeError(f"Missing super().__init__() in the __init__() method of {self.__class__.__name__}!")

        for observer in self._observers:
            observer.observe(self, *data_frames)

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

        Returns:
            Output of _process method
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
        self.observe(result)
        return result

    def is_functional(self):
        """
        Function that returns whether the operator is functional or infrastructure

        Returns:
            is_functional (bool):
                whether is_functional
        """
        return True
