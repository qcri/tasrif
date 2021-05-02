# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% language="html"
# <style>
# div.output_area pre {
#     white-space: pre;
# }
# </style>

# %%
from tasrif.processing_pipeline.pandas import JsonNormalizeOperator
data = [
    {
        "date": "2019-05-02",
        "main_sleep": True,
        "data": {
            "total_sleep": 365,
            "time_series": [{
                  "dateTime" : "2019-05-02T00:18:00.000",
                  "level" : "light",
                  "seconds" : 2130
                },{
                  "dateTime" : "2019-05-02T00:53:30.000",
                  "level" : "deep",
                  "seconds" : 540
                },{
                  "dateTime" : "2019-05-02T01:02:30.000",
                  "level" : "light",
                  "seconds" : 870
                },{
                  "dateTime" : "2019-05-02T01:17:00.000",
                  "level" : "rem",
                  "seconds" : 660
                },{
                  "dateTime" : "2019-05-02T01:28:00.000",
                  "level" : "light",
                  "seconds" : 1230
                },{
                  "dateTime" : "2019-05-02T01:48:30.000",
                  "level" : "wake",
                  "seconds" : 210
                }]
            }
    },
    {
        "date": "2019-04-29",
        "main_sleep": True,
        "data": {
              "total_sleep": 456,
              "time_series": [{
              "dateTime" : "2019-04-29T23:46:00.000",
              "level" : "wake",
              "seconds" : 300
            },{
              "dateTime" : "2019-04-29T23:51:00.000",
              "level" : "light",
              "seconds" : 660
            },{
              "dateTime" : "2019-04-30T00:02:00.000",
              "level" : "deep",
              "seconds" : 450
            },{
              "dateTime" : "2019-04-30T00:09:30.000",
              "level" : "light",
              "seconds" : 2070
            }]
        }
    }
]

op = JsonNormalizeOperator(record_path=['data', 'time_series'], meta=['date', 'main_sleep', ['data', 'total_sleep']])

df = op.process(data)

# %%
df
