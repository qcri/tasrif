<p align="center">
  <img width="75%" src="docs/TasrifLogo.svg" alt="Tasrif">
</p>
<p align="center">
  <b>A Python framework for processing wearable data in the health domain.</b>
</p>
<p align="center">
  <a href="https://tasrif.qcri.org">
    <img src="https://awesome.re/badge.svg" alt="SIHA">
  </a>
  <a href="https://www.python.org/">
    <img src="https://img.shields.io/badge/Made%20with-Python3-1f425f.svg" alt="Made with Python">
  </a>
  <a href="https://github.com/qcri/tasrif/graphs/commit-activity">
    <img src="https://img.shields.io/badge/Maintained%3F-yes-green.svg" alt="Maintenance">
  </a>
  <a href="https://github.com/qcri/tasrif/actions">
    <img src="https://github.com/qcri/tasrif/actions/workflows/ci.yml/badge.svg" alt="Workflow Status">
  </a>
  <a href="https://pypi.org/project/tasrif/">
    <img alt="PyPI - Downloads" src="https://img.shields.io/pypi/dm/tasrif?style=flat">
  </a>  
  <a href="https://pypi.org/project/tasrif/">
    <img alt="PyPI - Version" src="https://img.shields.io/pypi/v/tasrif">
  </a>
  <a href="https://GitHub.com/qcri/tasrif/watchers/">
    <img src="https://img.shields.io/github/watchers/qcri/tasrif?style=social&label=Watch&maxAge=2592000" alt="GitHub Watchers">
  </a>
  <a href="https://GitHub.com/qcri/tasrif/stargazers/">
    <img src="https://img.shields.io/github/stars/qcri/tasrif?style=social&label=Star&maxAge=2592000" alt="GitHub Stars">
  </a> 
</p>


#

Tasrif is a library for processing of eHealth data. It provides:

- A pipeline DSL for chaining together commonly used processing operations on time-series eHealth
  data, such as resampling, normalization, etc.
- DataReaders for reading eHealth datasets such as
  [MyHeartCounts](https://www.synapse.org/?source=post_page---------------------------#!Synapse:syn11269541/wiki/), [SleepHealth](https://www.synapse.org/#!Synapse:syn18492837/wiki/) and data from FitBit devices.

## Installation

To use Tasrif, you will need to have the package installed. Please follow the bellow steps to install Tasrif:


First, create a virtual environment using [venv](https://docs.python.org/3/library/venv.html) with a linux operating system machine, or with [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install)

```python
# Create a virtual environment
python3 -m venv tasrif-env

# Activate the virtual environment
source tasrif-env/bin/activate

# Upgrade pip
(tasrif-env) pip install --upgrade pip
```

Then, install Tasrif either from PyPI

```python
(tasrif-env) pip install tasrif
```

or install from source

```python
(tasrif-env) git clone https://github.com/qcri/tasrif
(tasrif-env) cd tasrif
(tasrif-env) pip install -e .

```

If no installation errors occur, see [Quick start by usecase](#quick-start-by-usecase) section to use Tasrif.

### Note on feature extraction using Tasrif

Due to some outdated internal Tasrif dependancies on Pypi, we have decided to place those dependancies in `requirements.txt`. Once those packages are updated in Pypi, we will move them back to `setup.py`. The current `requirements.txt` specifies the dependancies links directly from Github. If you plan to use the following two operators: `TSFreshFeatureExtractorOperator` or `CalculateTimeseriesPropertiesOperator`, you will need [TSFresh](https://github.com/blue-yonder/tsfresh) and [Kats](https://github.com/facebookresearch/Kats) packages installed, which can be done by running the following command

```python
(tasrif-env) MINIMAL_KATS=1 pip install -r requirements.txt
```

Note that `MINIMAL_KATS=1` is passed in the installation script to minimally install Kats. See [requirements.txt](requirements.txt) for details.

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

## Quick start by usecase

- [Reading data](#reading-data)
- [Compute statistics](#compute-statistics)
- [Extract features from existing columns](#extract-features-from-existing-columns)
- [Filter data](#filter-data)
- [Wrangle data](#wrangle-data)
- [Test prepared data](#test-prepared-data)
- [Create a pipeline to link the operators](#create-a-pipeline-to-link-the-operators)
- [Debug your pipeline](#debug-your-pipeline)
- [Define a custom operator](#define-a-custom-operator)
- [Other references](#other-references)


### Reading data

Reading a single csv file

```python
from tasrif.processing_pipeline.pandas import ReadCsvOperator

operator = ReadCsvOperator('/path/to/file')
df = operator.process()[0]
```

Reading multiple csvs in a folder

```python
from tasrif.processing_pipeline.pandas import ReadCsvOperator

operator = ReadCsvFolderOperator(name_pattern='/path/to/folder/*.csv'),
df = operator.process()[0]
```

by default, `ReadCsvFolderOperator` concatenates the csvs into one dataframe. if you would like to work on the csvs separately, you can pass the argument `concatenate=False` to `ReadCsvFolderOperator`, which returns a python generator that iterates the csvs.


Reading csvs referenced by a column in dataframe `df`

```python
import pandas as pd
from tasrif.processing_pipeline.custom import ReadNestedCsvOperator

df = pd.DataFrame({"name": ['Alfred', 'Roy'],
                   "age": [43, 32],
                   "csv_files_column": ['details1.csv', 'details2.csv']})

operator = ReadNestedCsvOperator(folder_path='/path/to/csv/files', field='csv_files_column')
generator = operator.process(df)[0]

for record, details in generator:
    print(record)
    print(details)
```

Reading json files referenced by a column in dataframe `df`

```python
import pandas as pd
from tasrif.processing_pipeline.custom import IterateJsonOperator

df = pd.DataFrame({"name": ['Alfred', 'Roy'],
                   "age": [43, 32],
                   "json_files_column": ['details1.json', 'details2.json']})

operator = IterateJsonOperator(folder_path='/path/to/json/files', field='json_files_column')
generator = operator.process(df)[0]

for record, details in generator:
    print(record)
    print(details)
```

### Compute statistics

Compute quick statistics using `StatisticsOperator`. `StatisticsOperator` includes counts of rows, missing data, duplicate rows, and others.

```python

import pandas as pd
from tasrif.processing_pipeline.custom import StatisticsOperator

df = pd.DataFrame( [
    ['2020-02-20', 1000, 1800, 1], ['2020-02-21', 5000, 2100, 1], ['2020-02-22', 10000, 2400, 1],
    ['2020-02-20', 1000, 1800, 1], ['2020-02-21', 5000, 2100, 1], ['2020-02-22', 10000, 2400, 1],
    ['2020-02-20', 0, 1600, 2], ['2020-02-21', 4000, 2000, 2], ['2020-02-22', 11000, 2400, 2],
    ['2020-02-20', None, 2000, 3], ['2020-02-21', 0, 2700, 3], ['2020-02-22', 15000, 3100, 3]],
columns=['Day', 'Steps', 'Calories', 'PersonId'])

filter_features = {
    'Steps': lambda x : x > 0
}

sop = StatisticsOperator(participant_identifier='PersonId', 
                         date_feature_name='Day', filter_features=filter_features)
sop.process(df)[0]
```

Or use `ParticipationOverviewOperator` to see statistics per participant. Pass the argument `overview_type="date_vs_features"` to compute statistics per date. See below

```python

from tasrif.processing_pipeline.custom import ParticipationOverviewOperator

sop = ParticipationOverviewOperator(participant_identifier='PersonId', 
                                    date_feature_name='Day', 
                                    overview_type='participant_vs_features')
sop.process(df)[0]


```

Use `AggregateOperator` if you require specific statistics for some columns


```python
from tasrif.processing_pipeline.custom import AggregateOperator

operator = AggregateOperator(groupby_feature_names ="PersonId",
                            aggregation_definition= {"Steps": ["mean", "std"],
                                                     "Calories:": ["sum"]
                                                    })
operator.process(df)[0]

```


### Extract features from existing columns

Convert time columns into cyclical features, which are more efficiently grasped by machine learning models

```python

from tasrif.processing_pipeline.custom import EncodeCyclicalFeaturesOperator

operator = EncodeCyclicalFeaturesOperator(date_feature_name="startTime", 
                                          category_definition="day")
operator.process(df)[0]
```


Extract timeseries features using `CalculateTimeseriesPropertiesOperator` which internally calls `kats` package

```python

from tasrif.processing_pipeline.custom import CalculateTimeseriesPropertiesOperator

operator = CalculateTimeseriesPropertiesOperator(date_feature_name="startTime", value_column='Steps')
operator.process(df)[0]

```

Extract using features using `tsfresh` package

```python

from tasrif.processing_pipeline.tsfresh import TSFreshFeatureExtractorOperator

operator = TSFreshFeatureExtractorOperator(seq_id_col="seq_id", date_feature_name='startTime', value_col='Steps')
operator.process(df)[0]

```

Note that `TSFreshFeatureExtractorOperator` requires a column`seq_id`. This column indicates which entities the time series belong to. Features will be extracted individually for each entity (id). The resulting feature matrix will contain one row per id. The column can be created manually or be created via `SlidingWindowOperator`.


### Filter data

filter rows, days, or participants with a custom condition using `FilterOperator`

```python
from tasrif.processing_pipeline.custom import FilterOperator

operator = FilterOperator(participant_identifier="PersonId",
                          date_feature_name="Hours",
                          epoch_filter=lambda df: df['Steps'] > 10,
                          day_filter={
                              "column": "Hours",
                              "filter": lambda x: x.count() < 10,
                              "consecutive_days": (7, 12) # 7 minimum consecutive days, and 12 max
                          },
                          filter_type="include")
operator.process(df)[0]
```

### Wrangle data

Add a column using `CreateFeatureOperator`

```python
import pandas as pd
from pandas import Timestamp

df = pd.DataFrame([
 [Timestamp('2016-12-31 00:00:00'), Timestamp('2017-01-01 09:03:00'), 5470, 2968, 1],
 [Timestamp('2017-01-01 00:00:00'), Timestamp('2017-01-01 23:44:00'), 9769, 2073, 1],
 [Timestamp('2017-01-02 00:00:00'), Timestamp('2017-01-02 16:54:00'), 9444, 2883, 1],
 [Timestamp('2017-01-03 00:00:00'), Timestamp('2017-01-05 22:49:00'), 20064, 2287, 1],
 [Timestamp('2017-01-04 00:00:00'), Timestamp('2017-01-06 07:27:00'),16771, 2716, 1]],
    columns = ['startTime', 'endTime', 'steps', 'calories', 'personId']
)

operator = CreateFeatureOperator(
   feature_name="duration",
   feature_creator=lambda df: df['endTime'] - df['startTime'])

operator.process(df)[0]

```

Upsample or downsample date features using `ResampleOperator`. The first argument `rule` can be minutes `min`, hours `H`, days `D`, and more. See details of resampling [here](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.resample.html) 

```python
from tasrif.processing_pipeline.custom import ResampleOperator

op = ResampleOperator('D', {'sleep_level': 'mean'})
op.process(df)[0]
```

Set the start hour of the day to some hour using `SetStartHourOfDayOperator`

```python
operator = SetStartHourOfDayOperator(date_feature_name='startTime',
                                     participant_identifier='personId',
                                     shift=6)
 
operator.process(df)[0]

```

a new column `shifted_time_col` will be created. This can be useful if the user wants to calculate statistics at a redefined times of the day instead of midnight-to-midnight (e.g. 8:00 AM - 8:00 AM).

Concatenate multiple dataframes or a generator using `ConcatOperator`

```python

from tasrif.processing_pipeline.pandas import ConcatOperator

df1 = df.copy()
df2 = df.copy()

concatenated_df = ConcatOperator().process(df1, df2)[0]
concatenated_df = ConcatOperator().process(generator)[0]
```

Normalize selected columns

```python
import pandas as pd
from tasrif.processing_pipeline.custom import NormalizeOperator
df = pd.DataFrame([
    [1, "2020-05-01 00:00:00", 10],
    [1, "2020-05-01 01:00:00", 15], 
    [1, "2020-05-01 03:00:00", 23], 
    [2, "2020-05-02 00:00:00", 17],
    [2, "2020-05-02 01:00:00", 11]],
    columns=['logId', 'timestamp', 'sleep_level'])

op = NormalizeOperator('all', 'minmax', {'feature_range': (0, 2)})
output = op.process(df)
```

Use the fit normalizer on different data using `NormalizeTransformOperator`

```python

trained_model = output1[0][1]
op = NormalizeTransformOperator('all', trained_model)
op.process(df.to_frame())
```

You can use `jqOperator` to process JSON data

```python
from tasrif.processing_pipeline.custom import JqOperator

op = JqOperator("map({date, sleep: .sleep[].sleep_data})")
op.process(df)[0]

```

### Test prepared data

See if your prepared data can act as an input to a machine learning model

```python
from tasrif.processing_pipeline.custom import LinearFitOperator
df = pd.DataFrame([
    [1, "2020-05-01 00:00:00", 10, 'poor'],
    [1, "2020-05-01 01:00:00", 15, 'poor'],
    [1, "2020-05-01 03:00:00", 23, 'good'],
    [2, "2020-05-02 00:00:00", 17, 'good'],
    [2, "2020-05-02 01:00:00", 11, 'poor']],
    columns=['logId', 'timestamp', 'sleep_level', 'sleep_quality'])

op = LinearFitOperator(feature_names='sleep_level',
                       target='sleep_quality',
                       target_type='categorical')
op.process(df)
```

### Create a pipeline to link the operators

Chain operators using `SequenceOperator`

```python
import pandas as pd
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.custom import AggregateOperator, CreateFeatureOperator
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator, SortOperator

pipeline = SequenceOperator([
    ConvertToDatetimeOperator(feature_names=["startTime"]),
    SetStartHourOfDayOperator(date_feature_name='startTime',
                                     participant_identifier='PersonId',
                                     shift=6),
    SortOperator(by='startTime'),
    AggregateOperator(groupby_feature_names ="PersonId",
                      aggregation_definition= {"Steps": ["mean", "std"], "Calories:": ["sum"]})

])

pipeline.process(df)
```


### Debug your pipeline

Tasrif contains observers under `tasrif/processing_pipeline/observers/` that are useful for seeing how the operators change your data. For instance, you can print the head of processed dataframe after every operator. You can do so by passing an `observer` to the `observers` argument in `SequenceOperator`.

```python

import pandas as pd
from tasrif.processing_pipeline.pandas import RenameOperator
from tasrif.processing_pipeline.observers import FunctionalObserver, Logger, GroupbyLogger
from tasrif.processing_pipeline import SequenceOperator, Observer

df = pd.DataFrame([
    [1, "2020-05-01 00:00:00", 1],
    [1, "2020-05-01 01:00:00", 1], 
    [1, "2020-05-01 03:00:00", 2], 
    [2, "2020-05-02 00:00:00", 1],
    [2, "2020-05-02 01:00:00", 1]],
    columns=['logId', 'timestamp', 'sleep_level'])

pipeline = SequenceOperator([RenameOperator(columns={"timestamp": "time"}), 
                             RenameOperator(columns={"time": "time_difference"})], 
                             observers=[Logger("head,tail")])
result = pipeline.process(df[0])
result

```

### Define a custom operator

Users can inherit from `MapProcessingOperator` to quickly build their own custom operators that perform map-like operations.

```python
from tasrif.processing_pipeline.map_processing_operator import MapProcessingOperator

class SizeOperator(MapProcessingOperator):
    def _processing_function(self, df):
        return df.size
```


### Other references

- You may examine `tasrif/processing_pipeline/test_scripts/` for other minimal examples of Tasrif's operators.
- Common Pandas functions can be found under `tasrif/processing_pipeline/pandas/`


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
   <a href="https://github.com/qcri/tasrif/network/dependencies">
   <img src="https://img.shields.io/github/contributors/qcri/tasrif" alt="Contributors">
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
