"""Module that defines the ComposeOperator class
"""

import ray
from tasrif.processing_pipeline.processing_operator import ProcessingOperator
from tasrif.processing_pipeline.parallel_operator import ParallelOperator

class ComposeOperator(ParallelOperator):
    """Class representing a composiition of processing operators. The same data flows to all the
    operators. The order is not important. The output of the `process` function is a composition of the
    results of all the containing operators
    """

    def __init__(self, processing_operators, observers=None, num_processes=1):
        """Constructs a compose operator from a list of operators

        Args:
            processing_operators : list[ProcessingOperator]
                Python list of processing operators
            observers (list[Observer]):
                Python list of observers
            num_processes: int
                number of logical processes to use to process the operator

        Raises:
            ValueError: Occurs when one of the objects in the specified list is not a ProcessingOperator

        Examples
        --------

        >>> import numpy as np
        >>> import pandas as pd
        >>> from tasrif.processing_pipeline.pandas import DropDuplicatesOperator, DropNAOperator
        >>> from tasrif.processing_pipeline import ComposeOperator
        >>> pipeline = ComposeOperator([DropDuplicatesOperator(), DropNAOperator()])
        >>> df = pd.DataFrame({"pid": ['001', '002', '003'],
        >>>                  "height": [np.nan, 188, 170],
        >>>                  "born": [pd.NaT, pd.Timestamp("1940-04-25"),
        >>>                           pd.NaT]})
        >>> pipeline.process(df)
        [(   pid  height       born
          0  001     NaN        NaT
          1  002   188.0 1940-04-25
          2  003   170.0        NaT,),
         (   pid  height       born
          1  002   188.0 1940-04-25,)]

        """
        super().__init__(num_processes)
        self._observers = []

        for operator in processing_operators:
            if not isinstance(operator, ProcessingOperator):
                raise ValueError("All operators in a pipeline must derive from ProcessingOperator!")

        self.processing_operators = processing_operators
        self.set_observers(observers)

    def set_observers(self, observers):
        if observers and not self._observers:
            self._observers = observers
            for operator in self.processing_operators:
                operator.set_observers(self._observers)

    def _process(self, *args):
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

    def _process_ray(self, *args):
        """
        Ray version of _process

        Args:
            *args (list of ProcessingOperator):
                Variable number of ProcessingOperator to be applied on a dataframe

        Returns:
            output (list[pd.DataFrame]):
                processed dataframes
        """
        output = []
        for operator in self.processing_operators:
            result = self._process_operator.remote(operator, *args)
            output.append(result)

        assert isinstance(output, list)
        assert all(isinstance(x, ray.ObjectID) for x in output)
        output = ray.get(output)

        return output

    def is_functional(self):
        """
        Function that returns whether the operator is functional or infrastructure

        Returns:
            is_functional (bool):
                whether is_functional
        """
        return False
