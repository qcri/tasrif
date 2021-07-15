
"""Module that defines the IterateOperator class
"""

from tasrif.processing_pipeline.processing_operator import ProcessingOperator

class IterateOperator(ProcessingOperator):
    """Class representing an iteration operation over the input data. Each incoming input dataframe is applied the
    processing operator supplied in the constructor. The order of output is the same as the input.
    """

    def __init__(self, operator):
        """Constructs a iterate operator

        Args:
            operator (ProcessingOperator):
                Operation to be applied to the input

        Raises:
            ValueError: Occurs when input argument is not a ProcessingOperator

        Examples
        --------
        >>> import pandas as pd
        >>> from tasrif.processing_pipeline import IterateOperator
        >>> from tasrif.processing_pipeline.pandas import SortOperator
        >>> df1 = pd.DataFrame({'id': [45, 91, 47, 27, 15]})
        >>> df2 = pd.DataFrame({'id': [2, 21, 56, 68, 8]})
        >>> op = IterateOperator(SortOperator(by='id'))
        >>> op.process(df1, df2)
        [   id
         4  15
         3  27
         0  45
         2  47
         1  91,
            id
         0   2
         4   8
         1  21
         2  56
         3  68]

        """
        if not isinstance(operator, ProcessingOperator):
            raise ValueError("All operators in a pipeline must derive from ProcessingOperator!")

        self.operator = operator

    def process(self, *dataframes):
        """Processes the dataframes with the operator. The operator is applied to each dataframe independently.

        Args:
            *dataframes (list[pd.DataFrame]):
                list of dataframes to be applied on by the processes

        Returns:
            output (list[pd.DataFrame]):
                processed dataframes

        """
        output = []
        for dataframe in dataframes:
            output += self.operator.process(dataframe)
        return output
