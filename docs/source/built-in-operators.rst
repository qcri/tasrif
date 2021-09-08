Built-In Operators
==================

Tasrif has many built-in operators that suit most eHealth processing workflows.

Currently, built-in Operators can be classified into three groups:

* **Pandas**
* **Kats**
* **TSFresh**
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
:code:`CalculateTimeSeriesOperator`. This operator is useful to extract useful features
, such as, seasonality strength, entropy (how predictable is a time-series), and more.

**Tsfresh Operators** are built on top of `TSFresh library`_. Currently, the only Operator present is the 
:code:`TSFreshFeatureExtractorOperator`. The operator extracts time-series features based on 
scalable hypothesis tests. The default features returned from the operators are 

.. code-block:: python

          >>> TSFRESH_FEATURES = {'agg_linear_trend': [{'attr': 'slope', 'chunk_len': 50, 'f_agg': 'mean'},
          ...                                          {'attr': 'slope', 'chunk_len': 10, 'f_agg': 'var'},
          ...                                          {'attr': 'slope', 'chunk_len': 5, 'f_agg': 'max'},
          ...                                          {'attr': 'slope', 'chunk_len': 5, 'f_agg': 'mean'},
          ...                                          {'attr': 'rvalue', 'chunk_len': 5, 'f_agg': 'max'},
          ...                                          {'attr': 'slope', 'chunk_len': 50, 'f_agg': 'var'},
          ...                                          {'attr': 'rvalue', 'chunk_len': 5, 'f_agg': 'mean'},
          ...                                          {'attr': 'rvalue', 'chunk_len': 5, 'f_agg': 'var'},
          ...                                          {'attr': 'slope', 'chunk_len': 10, 'f_agg': 'mean'},
          ...                                          {'attr': 'intercept', 'chunk_len': 5, 'f_agg': 'mean'},
          ...                                          {'attr': 'slope', 'chunk_len': 50, 'f_agg': 'max'},
          ...                                          {'attr': 'slope', 'chunk_len': 5, 'f_agg': 'var'},
          ...                                          {'attr': 'rvalue', 'chunk_len': 10, 'f_agg': 'var'},
          ...                                          {'attr': 'slope', 'chunk_len': 10, 'f_agg': 'max'},
          ...                                          {'attr': 'intercept', 'chunk_len': 5, 'f_agg': 'var'},
          ...                                          {'attr': 'rvalue', 'chunk_len': 10, 'f_agg': 'max'},
          ...                                          {'attr': 'intercept', 'chunk_len': 5, 'f_agg': 'max'},
          ...                                          {'attr': 'rvalue', 'chunk_len': 10, 'f_agg': 'mean'},
          ...                                          {'attr': 'intercept', 'chunk_len': 10, 'f_agg': 'mean'},
          ...                                          {'attr': 'intercept', 'chunk_len': 10, 'f_agg': 'var'},
          ...                                          {'attr': 'intercept', 'chunk_len': 10, 'f_agg': 'max'},
          ...                                          {'attr': 'rvalue', 'chunk_len': 50, 'f_agg': 'max'}],
          ...                      'linear_trend': [{'attr': 'rvalue'},
          ...                                       {'attr': 'slope'},
          ...                                       {'attr': 'intercept'}],
          ...                      'index_mass_quantile': [{'q': 0.4},
          ...                                              {'q': 0.7},
          ...                                              {'q': 0.6},
          ...                                              {'q': 0.8},
          ...                                              {'q': 0.3}],
          ...                      'cwt_coefficients': [{'coeff': 3, 'w': 2, 'widths': (2, 5, 10, 20)},
          ...                                           {'coeff': 7, 'w': 2, 'widths': (2, 5, 10, 20)}],
          ...                      'last_location_of_maximum': None,
          ...                      'fft_coefficient': [{'attr': 'imag', 'coeff': 1},
          ...                                          {'attr': 'imag', 'coeff': 8}],
          ...                      'first_location_of_maximum': None,
          ...                      'energy_ratio_by_chunks': [{'num_segments': 10,
          ...                                                  'segment_focus': 9}]}

Future operators may include one to extract relevant features from the time-series.

**Custom Operators** have custom processing functions built by the Tasrif team.
Examples include:

- :code:`AddDurationOperator`, for computing the duration between events in
  time series data.
- :code:`CreateFeatureOperator`, for adding new columns to DataFrames.
- :code:`StatisticsOperator`, for computing statistics such as row count
  and N/A counts for DataFrames.

.. _Facebook's Kats library: https://github.com/facebookresearch/Kats
.. _TSFresh library: https://github.com/blue-yonder/tsfresh
