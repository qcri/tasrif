"""Module that defines the ComposeOperator class
"""

from tasrif.processing_pipeline.processing_operator import ProcessingOperator

class ReduceOperator(ProcessingOperator):
    """Class representing a map operation on a list of data frames.
    """

    def __init__(self, operator, initial=None):
        """Constructs a compose operator from a list of operators

        Args:
            operator : ProcessingOperator
                Instance of a processing operator to be applied on each reduction step when iterating
                elements of the list passed as input.
            initial: object (usually df.DataFrame) that acts as a seed to the reduction procedure

        Raises:
            ValueError: Occurs when operator is not a ProcessingOperator

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
        if not isinstance(operator, ProcessingOperator):
            raise ValueError("operator must derive from ProcessingOperator!")

        self.initial = initial
        self.operator = operator


    def process(self, *args):
        """Processes a list of processing operators by applying the initialized operator
        to each element of the passed input list.

        Args:
            *args (list of pd.DataFrame):
                List of input data frames

        Returns:
            output (list[pd.DataFrame]):
                processed dataframes

        """
        if len(args) > 1:
            if self.initial:
                output = self.initial
            else:
                output = args[0]
            for arg in args[1:]:
                output = self.operator.process(output, arg)[0]

        return output
