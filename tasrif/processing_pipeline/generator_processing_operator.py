# pylint: disable=missing-module-docstring
from tasrif.processing_pipeline.processing_operator import ProcessingOperator

class GeneratorProcessingOperator(ProcessingOperator):
    """
    Operator that takes as input a list of generators and processes them according to a given function.
    This is in contrast to other operators that usually take as input a list of DataFrames.

    Example
    -------
    >>> from tasrif.processing_pipeline import GeneratorProcessingOperator
    >>>
    >>> def example_generator(upper_limit):
    ...     for num in range(0, upper_limit):
    ...         yield num
    ...
    >>> def processing_function(generator):
    ...     total = 0
    ...     for num in generator:
    ...         total += num
    ...     return total
    ...
    >>> operator = GeneratorProcessingOperator(processing_function)
    >>>
    >>> operator.process(example_generator(10), example_generator(20))
    [45, 190]
    """
    def __init__(self, processing_function):
        """
        Creates a new instance of GeneratorProcesssingOperator

        Args:
            processing_function (Callable[Generator]):
                A function that takes a generator and processes it to return an output.
        """
        self.processing_function = processing_function

    def process(self, *generators):
        output = []
        for generator in generators:
            result = self.processing_function(generator)
            output.append(result)
        return output
