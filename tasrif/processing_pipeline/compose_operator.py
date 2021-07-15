"""Module that defines the ComposeOperator class
"""

from tasrif.processing_pipeline.processing_operator import ProcessingOperator

class ComposeOperator(ProcessingOperator):
    """Class representing a composiition of processing operators. The same data flows to all the
    operators. The order is not important. The output of the `process` function is a composition of the
    results of all the containing operators
    """

    def __init__(self, processing_operators):
        """Constructs a compose operator from a list of operators

        Args:
            processing_operators : list[ProcessingOperator]
                Python list of processing operators

        Raises:
            ValueError: Occurs when one of the objects in the specified list is not a ProcessingOperator

        Examples
        --------

        >>> import numpy as np
        >>> import pandas as pd
        >>> from tasrif.processing_pipeline.pandas import DropDuplicatesOperator, DropNAOperator
        >>> from tasrif.processing_pipeline import ComposeOperator
        >>> pipeline = ComposeOperator([DropDuplicatesOperator(), DropNAOperator()])
        >>> df = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
        >>>                  "toy": [np.nan, 'Batmobile', 'Bullwhip'],
        >>>                  "born": [pd.NaT, pd.Timestamp("1940-04-25"),
        >>>                           pd.NaT]})
        >>> pipeline.process(df)
        [(       name        toy       born
          0    Alfred        NaN        NaT
          1    Batman  Batmobile 1940-04-25
          2  Catwoman   Bullwhip        NaT,),
         (     name        toy       born
          1  Batman  Batmobile 1940-04-25,)]

        """
        for operator in processing_operators:

            if not isinstance(operator, ProcessingOperator):
                raise ValueError("All operators in a pipeline must derive from ProcessingOperator!")

        self.processing_operators = processing_operators


    def process(self, *args):
        """Processes a list of processing operators. Input of an operator is received from the
        previous operator. The input for the first operator is passed to this function.
        The final result is the output of the final operation in the chain.

        Args:
            *args (list of ProcessingOperator):
                Variable number of ProcessingOperator to be applied on a dataframe

        Returns:
            output (list[pd.DataFrame]):
                processed dataframes

        """
        output = []
        for operator in self.processing_operators:
            result = operator.process(*args)
            output.append(result)

        return output
