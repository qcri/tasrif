Operators
=========

The basic unit of processing in Tasrif is the **Operator**. This is the
mechanism through which complex functionality is neatly packaged for use in
processing workflows.

Operators take as input and return as output `Pandas DataFrames`_. Operators can
also process multiple DataFrames at the same time.

As an example, consider the :code:`DropNAOperator` that can be used to drop rows
with missing values in input DataFrames:

.. code-block:: python

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.pandas import DropNAOperator

    >>> df1 = pd.DataFrame({
    ...     'Date':   ['05-06-2021', '06-06-2021', '07-06-2021', '08-06-2021'],
    ...     'Steps':  [        4500,         None,         5690,         6780]
    ... })

    >>> df2 = pd.DataFrame({
    ...     'Date':   ['12-07-2021', '13-07-2021', '14-07-2021', '15-07-2021'],
    ...     'Steps':  [        2100,         None,         None,         5400]
    ... })

    >>> operator = DropNAOperator()
    >>> dfs = operator.process(df1, df2)

    >>> dfs[0]
            Date   Steps
    0  05-06-2021  4500.0
    2  07-06-2021  5690.0
    3  08-06-2021  6780.0

    >>> dfs[1]
            Date   Steps
    0  12-07-2021  2100.0
    3  15-07-2021  5400.0

To use an Operator, we first instantiate it with its appropriate parameters.
Since the :code:`DropNAOperator` is built on top of
`pandas.DataFrame.dropna`_, the same parameters can also be passed in.

Next, we call the :code:`.process` method on the newly created Operator, and
then pass in the input DataFrames. The Operator drops each row with
missing values in both DataFrames, and returns them both in a list.

Operators are more useful when combined together to execute a processing
workflow. In the next section, we will see how to chain together multiple
Operators to form a processing **Pipeline**.

.. _Pandas DataFrames: https://pandas.pydata.org/pandas-docs/
    stable/user_guide/dsintro.html#dataframe

.. _pandas.DataFrame.dropna:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.dropna.html
