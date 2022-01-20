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

from tasrif.processing_pipeline.custom import ChangeDateFormatToStartToEnd
from tasrif.processing_pipeline.pandas import ReadCsvOperator

reader = ReadCsvOperator(os.environ['QUICKSTART'] + 'activity_long.csv')
df = reader.process()[0]

df = pd.DataFrame([
    [122, 1, "2016-03-13 02:39:00", 1],
    [122, 1, "2016-03-13 02:39:00", 1], 
    [122, 1, "2016-03-13 02:39:00", 1], 
    [122, 1, "2016-03-13 02:39:00", 1], 
    [122, 1, "2016-03-13 02:39:00", 1], 

    ],
    columns=['Id', 'logId', 'date', 'value'])

Id,logId,date,value
122,1,"2016-03-13 02:39:00",1
122,1,2016-03-13 02:40:00,1
122,1,2016-03-13 02:41:00,1
122,1,2016-03-13 02:42:00,1
122,1,2016-03-13 02:43:00,1
122,1,2016-03-13 02:44:00,1
122,1,2016-03-13 02:45:00,2
122,1,2016-03-13 02:46:00,2
122,1,2016-03-13 02:47:00,1
122,1,2016-03-13 02:48:00,1
122,1,2016-03-13 02:49:00,2
122,1,2016-03-13 02:50:00,2
122,1,2016-03-13 02:51:00,1
122,1,2016-03-13 02:52:00,1
122,1,2016-03-13 02:53:00,1
122,1,2016-03-13 02:54:00,1
122,1,2016-03-13 02:55:00,1
122,1,2016-03-13 02:56:00,1
122,1,2016-03-13 02:57:00,1
122,1,2016-03-13 02:58:00,1
122,1,2016-03-13 02:59:00,1
122,1,2016-03-13 03:00:00,1
122,1,2016-03-13 03:01:00,1

operator = ChangeDateFormatToStartToEnd(date_feature_name="date",
                                        participant_identifier=['Id', 'logId'])
df = operator.process(df)[0]
df
