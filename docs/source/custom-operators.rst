Custom Operators
================

Tasrif also allows the user to build their own Operators if they need to do so.

The interface for an Operator is simple:

.. code-block:: python

    class ProcessingOperator
        def process(self, *data_frames):
            pass

By subclassing from :code:`ProcessingOperator`, a user can build their own
Operators. For example, suppose we needed an Operator that computes the rows of
each DataFrame passed to it:

.. code-block:: python

    from tasrif.processing_pipeline import ProcessingOperator

    class RowCountOperator(ProcessingOperator):
        def process(self, *data_frames):
            output = []

            for df in data_frames:
                len(df.index)

            return output

Let's test our new Operator:

.. code-block:: python

    >>> df1 = pd.DataFrame({
    ...     'id':     [1,     2,       3      ],
    ...     'colors': ['red', 'white', 'green']
    ... })

    >>> df2 = pd.DataFrame({
    ...     'names': ['Fred', 'George', 'Harry'],
    ...     'colors': ['red', 'white', 'green']
    ... })

    >>> RowCountOperator().process(df1, df2)
    [3, 3]

To ease the creation of custom Operators, we have created two convenience
classes: :code:`MapProcessingOperator` and :code:`ReduceProcessingOperator`. As
their names suggest, these are Operators that can be subclassed to create custom
Operators that have map or reduce processing behavior.

For example, let's use the :code:`MapProcessingOperator` to build the
:code:`RowCountOperator`:

.. code-block:: python

    from tasrif.processing_pipeline import MapProcessingOperator

    class RowCountOperator(MapProcessingOperator):
        return len(df.index)

As you can see, these convenience classes are a quick way of creating simple,
custom Operators.
