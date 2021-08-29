Built-In Operators
==================

Tasrif has many built-in operators that suit most eHealth processing workflows.

Currently, built-in Operators can be classified into three groups:

* **Pandas**
* **Kats**
* **Custom**

**Pandas Operators**, as the name suggests, are Operators that are built on top of
the Pandas library. These Operators are mostly derived from the Pandas API, and
have been used to enrich Tasrif with commonly used operations on Pandas
DataFrames.Examples include the :code:`DropNaOperator`, :code:`ReadCsvOperator`
and :code:`ConvertToDatetimeOperator`, all of which are derived from `their
<https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.dropna.html>`_
`Pandas
<https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html>`_
`counterparts
<https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html>`_.

**Kats Operators** are built on top of `Facebook's Kats library`_ for time series
analysis. Currently, the only Operator present is the
:code:`CalculateTimeSeriesOperator`.

**Custom Operators** have custom processing functions built by the Tasrif team.
Examples include:

- :code:`AddDurationOperator`, for computing the duration between events in
  time series data.
- :code:`CreateFeatureOperator`, for adding new columns to DataFrames.
- :code:`StatisticsOperator`, for computing statistics such as row count
  and N/A counts for DataFrames.

.. _Facebook's Kats library: https://github.com/facebookresearch/Kats

