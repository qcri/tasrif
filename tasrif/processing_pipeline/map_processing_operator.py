"""Module that defines the MapProcessingOperator class
"""
import abc
from tasrif.processing_pipeline.processing_operator import ProcessingOperator

class MapProcessingOperator(ProcessingOperator, metaclass=abc.ABCMeta):
    """
    The MapProcessingOperator is a specialized ProcessingOperator, where a map
    function is applied on each element of a list of inputs. Users can inherit
    from this operator to quickly build their own custom operators that perform
    map-like operations.

    Examples
    --------
    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.map_processing_operator import MapProcessingOperator
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
    >>> class SizeOperator(MapProcessingOperator):
    >>>     def processing_function(self, df):
    >>>         return df.size
    >>>
    >>> SizeOperator().process(df0, df1)
    [15, 8]

    """
    @abc.abstractmethod
    def processing_function(self, element):
        """
        Map function to be applied to each element of a list of inputs.

        Args:
            element (Any):
                The element of the list the processing function acts on.
        """

    def process(self, *list_of_inputs):
        output = []
        for element in list_of_inputs:
            result = self.processing_function(element)
            output.append(result)
        return output
