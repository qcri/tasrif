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

# %%
import pandas as pd
from tasrif.processing_pipeline.custom import JqOperator
df = [
  {
    "date": "2020-01-01",
    "sleep": [
      {
        "sleep_data": [
          {
            "level": "rem",
            "minutes": 180
          },
          {
            "level": "deep",
            "minutes": 80
          },
          {
            "level": "light",
            "minutes": 300
          }
        ]
      }
    ]
  },
  {
    "date": "2020-01-02",
    "sleep": [
      {
        "sleep_data": [
          {
            "level": "rem",
            "minutes": 280
          },
          {
            "level": "deep",
            "minutes": 60
          },
          {
            "level": "light",
            "minutes": 200
          }
        ]
      }
    ]
  }
]



op = JqOperator("map({date, sleep: .sleep[].sleep_data})")

op.process(df)
