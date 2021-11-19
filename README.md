<p align="center">
  <img width="75%" src="./docs/TasrifLogo.svg" alt="Tasrif">
</p>
<p align="center">
  <b>A Python framework for processing wearable data in the health domain.</b>
</p>
<p align="center">
  <a href="https://siha.qcri.org">
    <img src="https://awesome.re/badge.svg" alt="SIHA">
  </a>
  <a href="https://www.python.org/">
    <img src="https://img.shields.io/badge/Made%20with-Python3-1f425f.svg" alt="Made with Python">
  </a>
  <a href="https://github.com/qcrisw/tasrif/graphs/commit-activity">
    <img src="https://img.shields.io/badge/Maintained%3F-yes-green.svg" alt="Maintenance">
  </a>
  <a href="https://github.com/qcrisw/tasrif/actions">
    <img src="https://github.com/qcrisw/tasrif/actions/workflows/ci.yml/badge.svg" alt="Workflow Status">
  </a>
   <a href="https://GitHub.com/qcrisw/tasrif/watchers/">
    <img src="https://img.shields.io/github/watchers/qcrisw/tasrif?style=social&label=Watch&maxAge=2592000" alt="GitHub Watchers">
  </a>
  </a>
   <a href="https://GitHub.com/qcrisw/tasrif/stargazers/">
    <img src="https://img.shields.io/github/stars/qcrisw/tasrif?style=social&label=Star&maxAge=2592000" alt="GitHub Stars">
  </a>
  </a>
</p>


#

Tasrif is a library for processing of eHealth data. It provides:

- A pipeline DSL for chaining together commonly used processing operations on time-series eHealth
  data, such as resampling, normalization, etc.
- DataReaders for reading eHealth datasets such as
  [MyHeartCounts](https://www.synapse.org/?source=post_page---------------------------#!Synapse:syn11269541/wiki/), [SleepHealth](https://www.synapse.org/#!Synapse:syn18492837/wiki/) and data from FitBit devices.

## Features

### Pipeline DSL

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
>>> from tasrif.processing_pipeline import SequenceOperator
>>> from tasrif.processing_pipeline.custom import AggregateOperator, CreateFeatureOperator
>>> from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SortOperator

>>> df0 = pd.DataFrame([
        ['15-07-2021', 'Doha', 25, 30],
        ['16-07-2021', 'Doha', 17, 50],
        ['15-07-2021', 'Dubai', 20, 40],
        ['16-07-2021', 'Dubai', 21, 42]],
        columns=['date', 'city', 'min_temp', 'max_temp'])

>>> pipeline = SequenceOperator([
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

### DataReaders

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
from tasrif.processing_pipeline import SequenceOperator
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

## Documentation

Tasrif's official documentation is hosted here: [https://tasrif.qcri.org](https://tasrif.qcri.org)

You can build the docs locally after installing the dependencies in `setup.py` and
`requirements.txt` by:

```
cd docs
make html
```

You can then browse through them by opening `docs/build/html/index.html` in a browser.

# Contributing

</a>
   <a href="https://github.com/qcrisw/tasrif/network/dependencies">
   <img src="https://img.shields.io/github/contributors/qcrisw/tasrif" alt="Contributors">
</a>


This project is much stronger with your collaboration. Be part of it!<br>
<b>Thank you all amazing contributors!</b>

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->

<a href="https://github.com/uabbas"><img src="https://avatars.githubusercontent.com/u/7748104?v=4" class="avatar-user" width="50px;"/></a> 
<a href="https://github.com/abalhomaid"><img src="https://avatars.githubusercontent.com/u/12021070?v=4" class="avatar-user" width="50px;"/></a> 
<a href="https://github.com/hashimmoosavi"><img src="https://avatars.githubusercontent.com/u/3678012?v=4" class="avatar-user" width="50px;"/></a> 
<a href="https://github.com/joaopalotti"><img src="https://avatars.githubusercontent.com/u/852343?s=400&v=4" class="avatar-user" width="50px;"/></a> 
<a href="https://github.com/fabubaker"><img src="https://avatars.githubusercontent.com/u/9405286?v=4" class="avatar-user" width="50px;"/></a> 
