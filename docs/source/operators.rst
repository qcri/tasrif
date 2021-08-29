Operators
=========

The basic unit of processing in Tasrif is the **Operator**. This is the
mechanism through which complex functionality is neatly packaged for use in
processing workflows.

Operators take as input and return as output `Pandas DataFrames`_. Operators can
also process multiple DataFrames at the same time.

As an example, consider the :code:`ReplaceOperator` that can be used to replace
specific values in input DataFrames:

.. code-block:: python

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.pandas import ReplaceOperator

    >>> df1 = pd.DataFrame({
    ...     'id':     [1,     2,       3      ],
    ...     'colors': ['red', 'white', 'green']
    ... })

    >>> df2 = pd.DataFrame({
    ...     'names': ['Fred', 'George', 'Harry'],
    ...     'colors': ['red', 'white', 'green']
    ... })

    >>> operator = ReplaceOperator(to_replace="red", value="blue")
    >>> dfs = operator.process(df1, df2)

    >>> dfs[0]
        id colors
    0   1   blue
    1   2  white
    2   3  green

    >>> dfs[1]
        names colors
    0    Fred   blue
    1  George  white
    2   Harry  green

To use an Operator, we first instantiate it with its appropriate parameters.
Since the :code:`ReplaceOperator` is built on top of
`pandas.DataFrame.replace`_, the same parameters can also be passed in.

Next, we call the :code:`.process` method on the newly created Operator, and
then pass in the input DataFrames. The Operator replaces all instances of :code:`red`
with :code:`green` in both DataFrames, and returns them both in a list.

Operators are more useful when combined together to execute a processing
workflow. In the next section, we will see how to chain together multiple
Operators to form a processing **Pipeline**.

.. _Pandas DataFrames: https://pandas.pydata.org/pandas-docs/
    stable/user_guide/dsintro.html#dataframe

.. _pandas.DataFrame.replace:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.replace.html
