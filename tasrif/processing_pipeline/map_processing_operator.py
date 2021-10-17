"""Module that defines the MapProcessingOperator class
"""
import abc
import ray
from tasrif.processing_pipeline.parallel_operator import ParallelOperator

class MapProcessingOperator(ParallelOperator, metaclass=abc.ABCMeta):
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
    >>>     def _processing_function(self, df):
    >>>         return df.size
    >>>
    >>> SizeOperator().process(df0, df1)
    [15, 8]

    """

    @abc.abstractmethod
    def _processing_function(self, element):
        """
        Map function to be applied to each element of a list of inputs.

        Args:
            element (Any):
                The element of the list the processing function acts on.
        """

    #pylint: disable=W0212
    @staticmethod
    @ray.remote
    def _processing_function_ray(instance, element):
        """
        Map function to be applied to each element of a list of inputs.

        Args:
            instance (Class):
                MapProcessingOperator instance
            element (Any):
                The element of the list the processing function acts on.

        Returns:
            processed list using `_processing_function`

        """
        return instance._processing_function(element)


    def _process(self, *list_of_inputs):
        output = []
        for element in list_of_inputs:
            result = self._processing_function(element)
            output.append(result)
        return output

    def _process_ray(self, *list_of_inputs):
        output = []
        for element in list_of_inputs:
            result = self._processing_function_ray.remote(self, element)
            output.append(result)

        assert isinstance(output, list)
        assert all(isinstance(x, ray.ObjectID) for x in output)
        output = ray.get(output)

        return output
