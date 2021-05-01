"""Module that defines the ComposeOperator class
"""

from tasrif.processing_pipeline import ProcessingOperator

class FunctionOperator(ProcessingOperator):
    """Class representing a function call on given arguments. 
    The output of the `process` function is a dataframe
    constructed via the given arguments.
    """

    def __init__(self, function: callable):
        """Constructs a function operator

        Parameters
        ----------
        function : python function

        Examples
        --------

        >>> import pandas as pd
        >>> from tasrif.processing_pipeline.custom import FunctionOperator
        >>> 
        >>> df0 = pd.DataFrame([['tom', 10, 2], ['nick', 15, 2], ['juli', 14, 12]],
        >>>                     columns=['name', 'work_hours', 'off_hours'])
        >>> 
        >>> df1 = pd.DataFrame([['ali', 'Admission', 23], ['tom', 'IT', 35], ['juli', 'Business', 32]],
        >>>                     columns=['name', 'department', 'age'])
        >>> 
        >>> print(df0)
        >>> print(df1)
        >>> 
        >>> operator = FunctionOperator(function=lambda df0, df1: df0[df0.name.isin(df1.name.values)])
        >>> combine = operator.process(df0, df1)
        >>> 
        >>> print()
        >>> print('Output:')
        >>> print(combine)

           name  work_hours  off_hours
        0   tom          10          2
        1  nick          15          2
        2  juli          14         12
           name department  age
        0   ali  Admission   23
        1   tom         IT   35
        2  juli   Business   32

        (   name  work_hours  off_hours
        0   tom          10          2
        2  juli          14         12,)


        """

        self.function = function


    def process(self, *args):
        """Provides args to the function
        """

        result = self.function(*args)
        return (result,)
