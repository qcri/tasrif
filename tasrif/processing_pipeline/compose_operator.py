"""Module that defines the ComposeOperator class
"""

from tasrif.processing_pipeline import ProcessingOperator

class ComposeOperator(ProcessingOperator):
    """Class representing a composiition of processing operators. The same data flows to all the
    operators. The order is not important. The output of the `process` function is a composition of the
    results of all the containing operators
    """

    def __init__(self, processing_operators):
        """Constructs a compose operator from a list of operators

        Parameters
        ----------
        processing_operators : list[ProcessingOperator]
            Python list of processing operators

        Raises
        ------
        ValueError
            Occurs when one of the objects in the specified list is not a ProcessingOperator

        Examples
        --------

        >>> from tasrif.processing_pipeline import ComposeOperator, DropDuplicatesOperator, DropNAOperator
        >>> pipeline = ComposeOperator([DropDuplicatesOperator(), DropNAOperator()])
        >>> df = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
        >>>                  "toy": [np.nan, 'Batmobile', 'Bullwhip'],
        >>>                  "born": [pd.NaT, pd.Timestamp("1940-04-25"),
        >>>                           pd.NaT]})
        >>> pipeline.process(df)

        """
        for operator in processing_operators:

            if not isinstance(operator, ProcessingOperator):
                raise ValueError("All operators in a pipeline must derive from ProcessingOperator!")

        self.processing_operators = processing_operators


    def process(self, *args):
        """Processes a list of processing operators. Input of an operator is received from the
        previous operator. The input for the first operator is passed to this function.
        The final result is the output of the final operation in the chain.
        """
        output = []
        for operator in self.processing_operators:
            result = operator.process(*args)
            output.append(result)

        return output