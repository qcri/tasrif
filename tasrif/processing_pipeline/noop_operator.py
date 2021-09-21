"""Module that defines the NoopOperator class
"""

from tasrif.processing_pipeline.processing_operator import ProcessingOperator

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
        (      0   1
         0   tom  10
         1  nick  15
         2  juli  14,)

        """
        super().__init__()

    def _process(self, *args): #pylint: disable=no-self-use
        """This function returned the received input without any changes (unmutated).

        Args:
            *args (list[pd.DataFrame]):
                list of dataframes

        Returns:
            args (list[pd.DataFrame]):
                the same input without change

        """
        return args
