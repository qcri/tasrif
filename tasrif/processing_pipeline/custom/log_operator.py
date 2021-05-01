"""Module that defines the ComposeOperator class
"""

from tasrif.processing_pipeline import ProcessingOperator


class LogOperator(ProcessingOperator):
    """Class to log the given input (currently, stdout). 
    The input data_frames not modified and are passed as an output
    """
    def __init__(self, function: callable, string=None):
        """Constructs a log operator

        Parameters
        ----------
        function : python function
        string : additional string to add before the print statement

        Examples
        --------

        >>> import pandas as pd
        >>> from tasrif.processing_pipeline.custom import LogOperator
        >>> 
        >>> df0 = pd.DataFrame([['tom', 10, 2], ['tom', 15, 2], ['juli', 14, 12]],
        >>>                     columns=['name', 'work_hours', 'off_hours'])
        >>> 
        >>> df1 = pd.DataFrame([['ali', 10, 2], ['juli', 15, 2], ['juli', 14, 12]],
        >>>                     columns=['name', 'work_hours', 'off_hours'])
        >>> 
        >>> print(df0)
        >>> print(df1)
        >>> 
        >>> print()
        >>> print('Unique names in dataframes:')
        >>> operator = LogOperator(lambda df: df.name.unique())
        >>> operator.process(df0, df1)

         name  work_hours  off_hours
        0   tom          10          2
        1   tom          15          2
        2  juli          14         12
           name  work_hours  off_hours
        0   ali          10          2
        1  juli          15          2
        2  juli          14         12

        Unique names in dataframes:
        ['tom' 'juli']
        ['ali' 'juli']


        """

        self.function = function
        self.string = string

    def process(self, *data_frames):
        """Provides a dataframe to the function, then prints the result
        """

        for dataframe in data_frames:
            result = self.function(dataframe)
            if self.string:
                print(self.string, result)
            else:
                print(result)

        return data_frames
