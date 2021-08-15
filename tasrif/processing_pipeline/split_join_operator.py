"""Module that defines the SequenceOperator class
"""

from tasrif.processing_pipeline.processing_operator import ProcessingOperator

class SplitJoinOperator(ProcessingOperator):
    """Class representing split join operation. The input coming into this operator is split into
    multiple branches represented by split operators that are passed in the constructor. The combined
    processed output of all the split operations is fed to a join operator.
    """

    def __init__(self, split_operators, join_operator, bind_list=None):
        """Constructs a split join operator using the provided arguments


        Args:
            split_operators (list[ProcessingOperator]):
                Python list of processing operators
            join_operator (ProcessingOperator):
                Operator that receives the combined output of split operators as input.
            bind_list (list[Integer]):
                Specifies the bind order of data passed to the process function to the input of the split operators.
                For example: a bind_order of [0, 1, 1] would bind first element of the arguments passed to process
                to the first split operator and the second element to the second and third element respectively.

        Raises:
            ValueError: Occurs when one of the objects in the split_operators list or the join_opertaor
                is not a ProcessingOperator

        Examples
        --------

        >>> import pandas as pd
        >>> from tasrif.processing_pipeline import  ProcessingOperator, SplitJoinOperator, \
        >>      NoopOperator, ProcessingPipeline
        >>>
        >>>
        >>> class ListofRangeOfNumbersOperator(ProcessingOperator):
        >>>
        >>>     def __init__(self, list_count, numbers_count):
        >>>
        >>>         self.list_count = list_count
        >>>         self.numbers_count = numbers_count
        >>>
        >>>     def process(self, *args):
        >>>         output = []
        >>>         for i in range(0, self.list_count):
        >>>             output.append(list(range(i*self.numbers_count, (i + 1)*self.numbers_count)))
        >>>
        >>>         return output
        >>>
        >>>
        >>> class MutiplybyOperator(ProcessingOperator):
        >>>
        >>>     def __init__(self, factor):
        >>>         self.factor = factor
        >>>
        >>>     def process(self, *args):
        >>>         output = []
        >>>         for arg in args:
        >>>             output_item = []
        >>>             for i in arg:
        >>>                 output_item.append(i*self.factor)
        >>>             output.append(output_item)
        >>>
        >>>         return output
        >>>
        >>>
        >>> class AddOperator(ProcessingOperator):
        >>>
        >>>     def __init__(self, data_count):
        >>>         self.data_count = data_count
        >>>
        >>>
        >>>     def process(self, *args):
        >>>         input_data = []
        >>>         for arg in args[0]:
        >>>             input_data.append(arg[0])
        >>>         input_data = (list(zip(*input_data)))
        >>>         output = []
        >>>         for arg in input_data:
        >>>             total = 0
        >>>             for i in arg:
        >>>                 total += i
        >>>             output.append(total)
        >>>
        >>>         return output
        >>>
        >>> pipeline = ProcessingPipeline([
        >>>     ListofRangeOfNumbersOperator(3, 10),
        >>>     SplitJoinOperator([
        >>>             NoopOperator(),
        >>>             MutiplybyOperator(2)],
        >>>         AddOperator(3))])
        >>>
        >>> pipeline.process()
        [20, 23, 26, 29, 32, 35, 38, 41, 44, 47]


        """
        for operator in split_operators:

            if not isinstance(operator, ProcessingOperator):
                raise ValueError("All operators in a pipeline must derive from ProcessingOperator!")

        if not isinstance(join_operator, ProcessingOperator):
            raise ValueError("join_operator must derive from ProcessingOperator!")

        self.split_operators = split_operators
        self.join_operator = join_operator
        self.bind_list = bind_list


    def process(self, *args):
        """Processes a list of processing operators. Input of an operator is received from the
        previous operator.

        Args:
            *args (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            pd.DataFrame -or- list[pd.DataFrame]
                Processed dataframe(s) resulting from applying the join_operator to the end result of executing
                all the split operators.
        """
        output = []
        data = args
        if self.bind_list:
            data = []
            for index in self.bind_list:
                data.append(args[index])

        for arg, operator in zip(data, self.split_operators):
            output.append(operator.process(arg))

        return self.join_operator.process(output)
