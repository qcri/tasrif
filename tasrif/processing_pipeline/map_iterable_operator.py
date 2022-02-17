"""Module that defines the MapProcessingOperator class
"""
import abc

import ray

from tasrif.processing_pipeline.map_processing_operator import MapProcessingOperator
from tasrif.processing_pipeline.processing_operator import ProcessingOperator


class MapIterableOperator(MapProcessingOperator):
    """
    The MapIterableOperator is a specialized MapProcessingOperator, where a map
    processing operator is applied on each element of a list of inputs.

    Examples
    --------
    >>> import pandas as pd
    >>> from tasrif.processing_pipeline import MapIterableOperator
    >>> from tasrif.processing_pipeline.pandas import DropNAOperator
    >>>
    >>>
    >>> df1 = pd.DataFrame({
    >>>     'Date':   ['05-06-2021', '06-06-2021', '07-06-2021', '08-06-2021'],
    >>>     'Steps':  [        4500,         None,         5690,         6780]
    >>>  })
    >>>
    >>> df2 = pd.DataFrame({
    >>>     'Date':   ['12-07-2021', '13-07-2021', '14-07-2021', '15-07-2021'],
    >>>     'Steps':  [        2100,         None,         None,         5400]
    >>>  })
    >>>
    >>>
    >>>
    >>> operator = MapIterableOperator(DropNAOperator(axis=0))
    >>> result = operator.process([df1, df2])
    >>>
    >>> result
    [(         Date   Steps
      0  05-06-2021  4500.0
      2  07-06-2021  5690.0
      3  08-06-2021  6780.0,),
     (         Date   Steps
      0  12-07-2021  2100.0
      3  15-07-2021  5400.0,)]
    """

    def __init__(self, processing_operator):

        super().__init__()

        if not isinstance(processing_operator, ProcessingOperator):
                raise ValueError("All operators in a pipeline must derive from ProcessingOperator!")

        self.processing_operator = processing_operator


    def _processing_function(self, element):
        return self.processing_operator.process(element)

    def _process(self, *list_of_inputs):
        MapIterableOperator._assert_input_is_iterable(list_of_inputs)

        list_of_inputs = list_of_inputs[0]
        output = []
        for element in list_of_inputs:
            result = self._processing_function(element)
            output.append(result)
        return output

    def _process_ray(self, *list_of_inputs):

        MapIterableOperator._assert_input_is_iterable(list_of_inputs)

        list_of_inputs = list_of_inputs[0]

        output = []
        for element in list_of_inputs:
            result = self._processing_function_ray.remote(self, element)
            output.append(result)

        assert isinstance(output, list)
        assert all(isinstance(x, ray.ObjectID) for x in output)
        output = ray.get(output)

        return output

    @staticmethod
    def _assert_input_is_iterable(list_of_inputs):
        valid = True
        if len(list_of_inputs) == 1:
            try:
                _ = iter(list_of_inputs[0])
            except TypeError:
                valid = False
        else:
            valid = False

        if not valid:
            raise ValueError("Invalid input to MapIterableOperator.process!")
