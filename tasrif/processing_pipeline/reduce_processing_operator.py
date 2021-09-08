"""Module that defines the ReduceProcessingOperator class
"""
import abc
from tasrif.processing_pipeline.processing_operator import ProcessingOperator

class ReduceProcessingOperator(ProcessingOperator, metaclass=abc.ABCMeta):
    """
    The ReduceProcessingOperator is a specialized ProcessingOperator, where a
    reduce function is applied on each element of a list of inputs. Users can
    inherit from this operator to quickly build their own custom operators that
    perform reduce-like operations.

    Attributes
    ----------
        initial (Any):
            The initial element passed to the reduce function.

    Examples
    --------
    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.reduce_processing_operator import ReduceProcessingOperator
    >>>
    >>> df0 = pd.DataFrame([[1, "2020-05-01 00:00:00", 1], [1, "2020-05-01 01:00:00", 1],
    >>>                     [1, "2020-05-01 03:00:00", 2], [2, "2020-05-02 00:00:00", 1],
    >>>                     [2, "2020-05-02 01:00:00", 1]],
    >>>                     columns=['logId', 'timestamp', 'sleep_level'])
    >>>
    >>> df1 = pd.DataFrame([['tom', 10],
    >>>                     ['Alfred', 15],
    >>>                     ['Alfred', 18],
    >>>                     ['juli', 14]],
    >>>                     columns=['name', 'age'])
    >>>
    >>> class AppendOperator(ReduceProcessingOperator):
    >>>     initial = pd.DataFrame([["Harry", "2020-05-01 00:00:00"]],
    >>>                             columns=["name", "timestamp"])
    >>>
    >>>     def _processing_function(self, df_to_append, dfs):
    >>>         return dfs.append(df_to_append)
    >>>
    >>> AppendOperator().process(df0, df1)
        name            timestamp  logId  sleep_level   age
    0   Harry  2020-05-01 00:00:00    NaN          NaN   NaN
    0     NaN  2020-05-01 00:00:00    1.0          1.0   NaN
    1     NaN  2020-05-01 01:00:00    1.0          1.0   NaN
    2     NaN  2020-05-01 03:00:00    1.0          2.0   NaN
    3     NaN  2020-05-02 00:00:00    2.0          1.0   NaN
    4     NaN  2020-05-02 01:00:00    2.0          1.0   NaN
    0     tom                  NaN    NaN          NaN  10.0
    1  Alfred                  NaN    NaN          NaN  15.0
    2  Alfred                  NaN    NaN          NaN  18.0
    3    juli                  NaN    NaN          NaN  14.0


    """
    initial = None

    @abc.abstractmethod
    def _processing_function(self, element, accumulated):
        """
        Reduce function to be applied to each element of a list of inputs.

        Args:
            element (Any):
                The element of the list the processing function acts on.
            accumulated (Any):
                The output value(s) accumulated so far as result of
                running processing_function on the list of inputs.
        """

    def _process(self, *list_of_inputs):
        if self.initial is not None:
            output = self.initial
            start_index = 0
        else:
            output = list_of_inputs[0]
            start_index = 1

        for element in list_of_inputs[start_index:]:
            output = self._processing_function(element, output)

        return output
