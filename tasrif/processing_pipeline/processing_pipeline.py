"""Module that defines the ProcessingPipeline class
The processing pipeline and the processing operator together define the basic framework
to define a processing pipeline
"""

from processing_pipeline.processing_operator import ProcessingOperator

class ProcessingPipeline:
    """Class representing a pipeline of processing operators. The definition of the pipeline
    is passed in the constructor as a list of ProcessingOperator objects.
    Data flows from one operator to another in a chained fashion.
    """

    def __init__(self, processing_operators):
        """Constructs a processing pipeline from a list of operators

        Parameters
        ----------
        processing_operators : list[ProcessingOperator]
            Python list of processing operators

        Raises
        ------
        ValueError
            Occurs when one of the objects in the specified list is not a ProcessingOperator

        Examples
        --------

        >>> from tasrif.processing_pipeline import ProcessingPipeline, DropDuplicates, DropNA
        >>> pipeline = ProcessingPipeline([DropDuplicates(), DropNA()])
        >>> pipeline.process(raw_df)

        """
        for operator in processing_operators:

            if not issubclass(operator, ProcessingOperator):
                raise ValueError("All operators in a pipeline must derive from ProcessingOperator!")

        self.processing_operators = processing_operators


    def process(self, *args):
        """Processes a list of processing operators. Input of an operator is received from the
        previous operator. The input for the first operator is passed to this function.
        The final result is the output of the final operation in the chain.
        """
        data = args
        for operator in self.processing_operators:
            data = operator.process(*data)

        return data
