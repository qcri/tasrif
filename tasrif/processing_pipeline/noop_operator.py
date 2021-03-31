"""Module that defines the NoopOperator class
"""

from tasrif.processing_pipeline import ProcessingOperator

class NoopOperator(ProcessingOperator):
    """Class representing a noop operator. This operator does nothing to the input and simply passes the input.
    """

    def __init__(self):
        """Constructs a noop operator

        Examples
        --------

        >>> from tasrif.processing_pipeline import NoopOperator
        >>> df = pd.DataFrame([['tom', 10], ['nick', 15], ['juli', 14]])
        >>> operator = NoopOperator()
        >>> operator.process(df)

        """

    def process(self, *args): #pylint: disable=no-self-use
        """This function returned the received input without any changes (unmutated).
        """
        return args