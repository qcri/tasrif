DataReaders
===========

DataReaders are a type of built-in Operator used to import data from popular
eHealth datasets into Tasrif. They are essentially operators that act as
specialized inputs for the pipeline.

Supported datasets include:

- MyHeartCounts
- Data from FitBit devices
- SIHA
- SleepHealth
- Withings
- ZenodoFitBit

DataReaders are instantiated with the path to the file/folder containing the
dataset, and any other optional parameters.

For example, here's a pipeline that uses body measurement data obtained from
FitBit devices:

.. code-block:: python

    >>> from tasrif.processing_pipeline import ProcessingPipeline
    >>> from tasrif.data_readers.fitbit_interday_dataset import FitbitInterdayDataset
    >>> from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SetIndexOperator

    >>> interday_folder_path = "path/to/data/from/FitBit/device"

    >>> pipeline = ProcessingPipeline([
    ...    FitbitInterdayDataset(
    ...             interday_folder_path,
    ...             table_name="Body"
    ...    ),
    ...    ConvertToDatetimeOperator(
    ...             feature_names=['Date'],
    ...             infer_datetime_format=True
    ...    ),
    ...    SetIndexOperator('Date')
    ... ])

    >>> dfs = pipeline.process()
    >>> dfs[0]
                Weight    BMI     Fat
    Date
    2019-07-01   84.02  29.77  30.103
    2019-07-02   83.93  29.74  30.103
    2019-07-03   83.85  29.71  30.103
    2019-07-04   83.76  29.68  30.103
    2019-07-05   83.68  29.65  30.103
    2019-07-06   83.59  29.62  30.103
    2019-07-07   83.50  29.58  30.103
    2019-07-08   83.51  29.59  30.028

Most DataReaders are straightforward and simply read the dataset files into
DataFrames. However, other DataReaders perform more complex processing for
datasets that need parsing before they can be used in a Tasrif pipeline.
