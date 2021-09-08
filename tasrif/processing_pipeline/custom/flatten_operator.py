"""
Module that defines that FlattenOperator class.
"""
from tasrif.processing_pipeline import ReduceProcessingOperator

class FlattenOperator(ReduceProcessingOperator):
    """
    The FlattenOperator takes as input multiple lists, and flattens all of them
    so that all the elements are contained in a single output list.

    Examples
    --------
    >>> from tasrif.processing_pipeline.custom import FlattenOperator
    >>>
    >>> FlattenOperator().process([1,2,3], [4,5,6], [7,8,9])
    [1, 2, 3, 4, 5, 6, 7, 8, 9]

    """
    def _processing_function(self, list_to_flatten, flattened):
        return flattened + list_to_flatten
