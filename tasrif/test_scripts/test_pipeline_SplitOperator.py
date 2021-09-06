import pandas as pd
from tasrif.processing_pipeline import SplitOperator
from tasrif.processing_pipeline.pandas import DropNAOperator, DropDuplicatesOperator

df0 = pd.DataFrame({
    'Date':  ['05-06-2021', '06-06-2021', '07-06-2021', '08-06-2021'],
    'Steps': [       pd.NA,         2000,        pd.NA,         4000]
})

df1 = pd.DataFrame({
    'Date':  ['05-06-2021', '06-06-2021', '06-06-2021', '07-06-2021', '07-06-2021', '08-06-2021'],
    'Steps': [       pd.NA,         2000,         2000,        pd.NA,        pd.NA,         4000]
})

operator = SplitOperator([
        DropNAOperator(),
        DropDuplicatesOperator()
    ])

operator.process(df0, df1)
