"""Module that defines the SequenceOperator class
"""

import ray
from tasrif.processing_pipeline.processing_operator import ProcessingOperator
from tasrif.processing_pipeline.parallel_operator import ParallelOperator

class SplitOperator(ParallelOperator):
    """Class representing a split operation. The input coming into this operator is split into
    multiple branches represented by split operators that are passed in the constructor.
    """

    def __init__(self, split_operators, bind_list=None, observers=None, num_processes=1):
        """Constructs a split operator using the provided arguments

        Args:
            split_operators (list[ProcessingOperator]):
                Python list of processing operators
            bind_list (list[Integer]):
                Specifies the bind order of data passed to the split operators, with each value in the bind_list
                corresponding to the index of the argument for the operator at that index.
                For example: a bind_order of [0, 1, 1] means that the first operator receives the first
                argument (index 0) and the second and third operator receives the second argument (index 1).
                Note that an error is raised if len(bind_list) != len(split_operators).
                If no bind_list is passed, arguments are passed in the same order as they are received
                (representing a bind_list of [0, 1, 2, ...]).
            observers (list[Observer]):
                Python list of observers
            num_processes: int
                number of logical processes to use to process the operator

        Raises:
            ValueError: Occurs when one of the objects in the split_operators list is not a ProcessingOperator.
            ValueError: If the number of operators does not match the number of elements in the bind_list.

        Examples
        --------

        >>> import pandas as pd
        >>> from tasrif.processing_pipeline import SplitOperator
        >>> from tasrif.processing_pipeline.pandas import DropNAOperator, DropDuplicatesOperator

        >>> df0 = pd.DataFrame({
        ...     'Date':  ['05-06-2021', '06-06-2021', '07-06-2021', '08-06-2021'],
        ...     'Steps': [       pd.NA,         2000,        pd.NA,         4000]
        ... })

        >>> df1 = pd.DataFrame({
        ... 'Date':  ['05-06-2021', '06-06-2021', '06-06-2021', '07-06-2021', '07-06-2021', '08-06-2021'],
        ... 'Steps': [       pd.NA,         2000,         2000,        pd.NA,        pd.NA,         4000]
        ... })

        >>> operator = SplitOperator([
        ...     DropNAOperator(),
        ...     DropDuplicatesOperator()
        ... ])

        >>> operator.process(df0, df1)
            [(         Date Steps
            1  06-06-2021  2000
            3  08-06-2021  4000,),
            (         Date Steps
            0  05-06-2021  <NA>
            1  06-06-2021  2000
            3  07-06-2021  <NA>
            5  08-06-2021  4000,)]
        """
        super().__init__(num_processes)
        self._observers = []
        for operator in split_operators:
            if not isinstance(operator, ProcessingOperator):
                raise ValueError("All split operators must derive from ProcessingOperator!")

        if bind_list and (len(split_operators) != len(bind_list)):
            raise ValueError("Length of split_operators must equal length of bind_list!")

        self.split_operators = split_operators
        self.bind_list = bind_list
        self.set_observers(observers)

    def set_observers(self, observers):
        if observers and not self._observers:
            self._observers = observers
            for operator in self.split_operators:
                operator.set_observers(self._observers)

    def _process(self, *args):
        """Processes a list of processing operators. Input of an operator is received from the
        previous operator.

        Args:
            *args (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            list[pd.DataFrame]
                Processed dataframe(s) resulting from distributing the inputs to the split_operators.
        """
        output = []
        data = args
        if self.bind_list:
            data = []
            for index in self.bind_list:
                data.append(args[index])

        for arg, operator in zip(data, self.split_operators):
            output.append(operator.process(arg))

        return output

    def _process_ray(self, *args):
        """
        Ray version of _process

        Args:
            *args (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            list[pd.DataFrame]
                Processed dataframe(s) resulting from distributing the inputs to the split_operators.
        """
        output = []
        data = args
        if self.bind_list:
            data = []
            for index in self.bind_list:
                data.append(args[index])

        for arg, operator in zip(data, self.split_operators):
            result = self._process_operator.remote(operator, arg)
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
