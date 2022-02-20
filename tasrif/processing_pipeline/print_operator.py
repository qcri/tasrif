"""Module that defines the PrintOperator class
"""

from tasrif.processing_pipeline.processing_operator import ProcessingOperator


class PrintOperator(ProcessingOperator):
    """Class representing a print operator. This operator does prints the input to the console and reutnr it."""

    def __init__(self, method=""):
        """
        The constructor of the PrintOperator class will provide options to configure the
        operation. The logging is invoked via the observe method and the data to be
        logged is passed to the observe method.

        Args:
            method (String):
                Logging method to log the dataframe
                Options: "head", "tail", "info"
        """

        super().__init__()

        self._logging_methods = []

        if method:
            self._logging_methods = method.split(",")

    def _process(self, *args):  # pylint: disable=no-self-use
        for data_frame in args:
            if self._logging_methods:
                for logging_method in self._logging_methods:
                    print(getattr(data_frame[0], logging_method)())
            else:
                print(data_frame)
        return args
