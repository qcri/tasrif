# Tasrif

Tasrif is a library for processing of eHealth data. It provides:

- A pipeline DSL for chaining together commonly used processing operations on time-series eHealth
  data, such as resampling, normalization, etc.
- DataReaders for reading eHealth datasets such as
  [MyHeartCounts](https://www.synapse.org/?source=post_page---------------------------#!Synapse:syn11269541/wiki/), [SleepHealth](https://www.synapse.org/#!Synapse:syn18492837/wiki/) and data from FitBit devices.

## Getting started

### Installation

To use Tasrif, you will need to have the package installed. Please follow the bellow steps to install Tasrif:


First, create a virtual environment using [venv](https://docs.python.org/3/library/venv.html) with a linux operating system machine, or with [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install)

```python
# Create a virtual environment
python3 -m venv tasrif-env

# Activate the virtual environment
source tasrif-env/bin/activat
```

Then, install Tasrif either from PyPI

```python
(tasrif-env) MINIMAL=1 pip install tasrif
```

or install from source

```python
(tasrif-env) git clone https://github.com/qcrisw/tasrif
(tasrif-env) cd tasrif
(tasrif-env) MINIMAL=1 pip install -e .

```

Note that `MINIMAL=1` is passed in the installation script to minimally install one of Tasrif's internal dependancies. See [setup.py](setup.py) for details.

### Quick start by usecase

Once Tasrif is installed, import Tasrif via

```python
import Tasrif
```


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



- Reading data
    + IterateJSON
- Concatenate them or process them individually
- See statistics
- add features 
    + CreateFeatureOperator
    + encode cyclical features
    + tsfresh
    + kats
- FilterOperator
- AggregateOperator
    + Suppose that ...
- DataWrangling
    + JSON data
        * jqOperator
    + SetStartHourOfDay
    + Resample
    + NormalizeOperator
- Quickly test your prepared data
    + linear fit operator
- Add final pipeline
    + Link to Colab
- Debug your pipeline
    + Observers
    + Explain how, and not what
- Other references
    + You may examine `tasrif/processing_pipeline/test_scripts/` for other minimal examples of Tasrif's operators.


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
