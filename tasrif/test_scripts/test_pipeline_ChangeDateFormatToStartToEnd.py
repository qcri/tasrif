# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import pandas as pd
import os

from tasrif.processing_pipeline.custom import ChangeDateFormatToStartToEnd
from tasrif.processing_pipeline.pandas import ReadCsvOperator

print(os.getcwd())
reader = ReadCsvOperator(os.environ['QUICKSTART'] + 'activity_long.csv')
df = reader.process()[0]

operator = ChangeDateFormatToStartToEnd(date_feature_name="date",
                                        participant_identifier=['Id', 'logId'])
df = operator.process(df)[0]
df
