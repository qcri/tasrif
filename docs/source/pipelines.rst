Pipelines
=========

**Pipelines** are used to gather multiple Operators together to create a
processing workflow.

Each Operator in a Pipeline is executed sequentially, with the output from one
Operator becoming the input for the next.

As a follow up to the previous example, let's replace all instances of
:code:`red` with :code:`blue`, concatenate both DataFrames and then group rows
by their colors, all using Tasrif Operators wrapped in a Pipeline:

.. code-block:: python

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.pandas import ReplaceOperator, \
    ...                                               ConcatOperator, \
    ...                                               GroupbyOperator
    >>> from tasrif.processing_pipeline import ProcessingPipeline

    >>> df1 = pd.DataFrame({
    ...     'id':     [1,     2,        3     ],
    ...     'colors': ['red', 'white', 'green']
    ... })

    >>> df2 = pd.DataFrame({
    ...     'names': ['Fred', 'George', 'Harry'],
    ...     'colors': ['red', 'white', 'green']
    ... })

    >>> pipeline = ProcessingPipeline([
    ...        ReplaceOperator(
    ...            to_replace='red',
    ...            value='blue'
    ...        ),
    ...        ConcatOperator(),
    ...        GroupbyOperator(by='colors')
    ... ])

    >>> groups = pipeline.process(df1, df2)
    >>> groups.get_group('blue')
        id colors names
    0  1.0   blue   NaN
    0  NaN   blue  Fred

As before, we instantiate all Operators with their appropriate parameters,
except this time, they are grouped together within the
:code:`ProcessingPipeline`.

Finally, the :code:`.process` method is called on the pipeline instead of the
operators. This causes the pipeline to execute the operators in sequential
order. Essentially, the pipeline by itself can be considered as an Operator, as
it is simply a composition of other Operators.
