# Tasrif

Tasrif is a library for processing of eHealth data. It provides:

- A pipeline DSL for chaining together commonly used processing operations on time-series eHealth
  data, such as resampling, normalization, etc.
- DataReaders for reading eHealth datasets such as
  [MyHeartCounts](https://www.synapse.org/?source=post_page---------------------------#!Synapse:syn11269541/wiki/), [SleepHealth](https://www.synapse.org/#!Synapse:syn18492837/wiki/) and data from FitBit devices.

# Features

## Pipeline DSL

Tasrif provies a variety of processing operators that can be chained together in a pipeline. The
operators themselves take as input and output [Pandas](https://pandas.pydata.org/)
[DataFrames](https://pandas.pydata.org/pandas-docs/stable/user_guide/dsintro.html#dataframe).

For example, consider the `AggregateOperator`:

```python
import pandas as pd
from tasrif.processing_pipeline.custom import AggregateOperator
from tasrif.processing_pipeline import DropNAOperator

>>> df0 = pd.DataFrame([
        ['Doha', 25, 30],
        ['Doha', 17, 50],
        ['Dubai', 20, 40],
        ['Dubai', 21, 42]],
        columns=['city', 'min_temp', 'max_temp'])

>>> operator = AggregateOperator(
    groupby_feature_names="city",
    aggregation_definition={"min_temp": ["mean", "std"]})

>>> df0 = operator.process(df0)

>>> df0
[    city  min_temp_mean  min_temp_std
0   Doha           21.0      5.656854
1  Dubai           20.5      0.707107]
```

Operators are meant to be used as part of a pipeline, where they can be chained together for
sequential processing of data:

```python
>>> import pandas as pd
>>> from tasrif.processing_pipeline import ProcessingPipeline
>>> from tasrif.processing_pipeline.custom import AggregateOperator, CreateFeatureOperator
>>> from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SortOperator

>>> df0 = pd.DataFrame([
        ['15-07-2021', 'Doha', 25, 30],
        ['16-07-2021', 'Doha', 17, 50],
        ['15-07-2021', 'Dubai', 20, 40],
        ['16-07-2021', 'Dubai', 21, 42]],
        columns=['date', 'city', 'min_temp', 'max_temp'])

>>> pipeline = ProcessingPipeline([
        ConvertToDatetimeOperator(feature_names=["date"]),
        CreateFeatureOperator(
            feature_name='avg_temp',
            feature_creator=lambda df: (df['min_temp'] + df['max_temp'])/2),
        SortOperator(by='avg_temp')
    ])

>>> pipeline.process(df0)
[        date   city  min_temp  max_temp  avg_temp
0 2021-07-15   Doha        25        30      27.5
2 2021-07-15  Dubai        20        40      30.0
3 2021-07-16  Dubai        21        42      31.5
1 2021-07-16   Doha        17        50      33.5]
```

## DataReaders

Tasrif also comes with DataReader classes for importing various eHealth datasets into pipelines.
These readers preprocess the raw data and convert them into a DataFrame for downstream processing in a pipeline.

Supported datasets include:
- [MyHeartCounts](https://www.synapse.org/?source=post_page---------------------------#!Synapse:syn11269541/wiki/)
- [SleepHealth](https://www.synapse.org/#!Synapse:syn18492837/wiki/)
- [Zenodo FitBit](https://zenodo.org/record/53894)
- Export data from FitBit devices
- Export data from Withings devices
- ...and more

DataReaders can be used by treating them as source operators in a pipeline:

```python
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.my_heart_counts import DayOneSurveyDataset
from tasrif.processing_pipeline import DropNAOperator

day_one_survey_path = <path to MyHeartCounts DayOneSurvey file>

pipeline = Pipeline([
    DayOneSurveyDataset(day_one_survey_path),
    DropNAOperator,
    SetIndexOperator('healthCode'),
])

pipeline.process()
```

