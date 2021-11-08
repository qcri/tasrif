"""Module that defines the SequenceOperator class
"""
from tasrif.processing_pipeline.processing_operator import ProcessingOperator

class ScopedProcessingOperator(ProcessingOperator):
    """ Class represented a scoped operation where the operation is strictly defined as a lambda function
    taking any arguments and returning a ProcessingOperator
    """

    def __init__(self, operation, observers=None):
        """Constructs a scoped processing operator where the operation is a lambda function

        Args:
            operation (lambda Any: ProcessingOperator):
                Lambda returning a ProcessingOperator
            observers (list[Observer]):
                Python list of observers

        Raises:
            ValueError: Occurs when operation is not a lambda function

        Examples
        --------
        >>> df1 = pd.DataFrame({'id': [1, 2, 3], 'cities': ['Rome', 'Barcelona', 'Stockholm']})
        >>> df2 = pd.DataFrame({'id': [4, 5, 6], 'cities': ['Doha', 'Vienna', 'Belo Horizonte']})

        >>> class TrainingOperator(ProcessingOperator):
        >>>     def __init__(self, model, x=1, y=2):
        >>>         super().__init__()
        >>>         self.model = model
        >>>         self.x = x
        >>>         self.y = y
        >>>     def _process(self, *args):
        >>>         self.model.value = {'x': self.x, 'y': self.y}
        >>>         return args
        >>> class PredictionOperator(ProcessingOperator):
        >>>     def __init__(self, model):
        >>>         super().__init__()
        >>>         self.model = model
        >>>     def _process(self, *args):
        >>>         print(self.model.value)
        >>>         return args
        >>> modela = Variable(None)
        >>> s = SequenceOperator(processing_operators=[
        >>>     TrainingOperator(model=modela),
        >>>     PredictionOperator(model=modela),
        >>>     ScopedProcessingOperator(lambda modelb=Variable():
        >>>         SequenceOperator(processing_operators=[
        >>>             TrainingOperator(model=modelb),
        >>>             PredictionOperator(model=modelb),
        >>>             ScopedProcessingOperator(lambda :
        >>>                 SequenceOperator(processing_operators=[
        >>>                     TrainingOperator(model=modelb),
        >>>                     PredictionOperator(model=modelb)]))
        >>>         ])
        >>>     )
        >>> ])
        """

        super().__init__()

        if not (callable(operation) and operation.__name__ == "<lambda>"):
            raise ValueError(f"Argument operation of {__class__.__name__} " +
                       "must be a lambda function returning a ProcessingOperator")

        self.operation = operation
        self._observers = []
        self.set_observers(observers)

    def set_observers(self, observers):
        if observers and not self._observers:
            self._observers = observers

    def _process(self, *args):
        """Processes a list of processing operators. Input of an operator is received from the
        previous operator. The input for the first operator is passed to this function.
        The final result is the output of the final operation in the chain.

        Args:
            *args (list of ProcessingOperator):
                Variable number of ProcessingOperator to be applied on a dataframe

        Returns:
            data (pd.DataFrame):
                processed dataframe

        """
        data = args
        operator = self.operation()
        operator.set_observers(self._observers)
        data = operator.process(*data)

        return data

    def is_functional(self):
        """
        Function that returns whether the operator is functional or infrastructure

        Returns:
            is_functional (bool):
                whether is_functional
        """
        return False
