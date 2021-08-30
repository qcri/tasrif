Pipelines
=========

**Pipelines** are used to gather multiple Operators together to create a
processing workflow.

Each Operator in a Pipeline is executed sequentially, with the output from one
Operator becoming the input for the next.

As a follow up to the previous example, let's do the following using Tasrif
Operators wrapped in a Pipeline:

1. Drop all rows with missing values
2. Concatenate both DataFrames
3. Get the mean step count

.. code-block:: python

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.pandas import DropNAOperator, \
    ...                                               ConcatOperator, \
    ...                                               MeanOperator
    >>> from tasrif.processing_pipeline import ProcessingPipeline

    >>> df1 = pd.DataFrame({
    ...     'Date':   ['05-06-2021', '06-06-2021', '07-06-2021', '08-06-2021'],
    ...     'Steps':  [        4500,         None,         5690,         6780]
    ... })

    >>> df2 = pd.DataFrame({
    ...     'Date':   ['12-07-2021', '13-07-2021', '14-07-2021', '15-07-2021'],
    ...     'Steps':  [        2100,         None,         None,         5400]
    ... })

    >>> pipeline = ProcessingPipeline([
    ...        DropNAOperator(),
    ...        ConcatOperator(),
    ...        MeanOperator()
    ... ])

    >>> results = pipeline.process(df1, df2)
    >>> results[0]
    Steps    4894.0
    dtype: float64


As before, we instantiate all Operators with their appropriate parameters,
except this time, they are grouped together within the
:code:`ProcessingPipeline`.

Finally, the :code:`.process` method is called on the pipeline instead of the
operators. This causes the pipeline to execute the operators in sequential
order. Essentially, the pipeline by itself can be considered as an Operator, as
it is simply a composition of other Operators.
