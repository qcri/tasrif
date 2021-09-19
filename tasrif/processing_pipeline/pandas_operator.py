"""Module that defines the PandasOperator class
"""
from tasrif.processing_pipeline.processing_operator import ProcessingOperator

class PandasOperator(ProcessingOperator):
    """Interface specification of a pandas operator
    """

    def __init__(self, kwargs):

        super().__init__(observers=kwargs.get('observers', []))
        if 'observers' in kwargs:
            del kwargs['observers']
