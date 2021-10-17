# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
import numpy as np

from tasrif.processing_pipeline.custom import AggregateOperator
from tasrif.processing_pipeline.custom import LinearFitOperator

df = pd.DataFrame([['Doha', 25, 30], ['Doha', 17, 50], ['Dubai', 20, 40], ['Dubai', 21, 42]],
                     columns=['city', 'min_temp', 'max_temp'])

# %%
operator = AggregateOperator(groupby_feature_names ="city",
                             aggregation_definition= {"min_temp": ["mean", "std"],
                                                      "r2,_,intercept": LinearFitOperator(feature_names='min_temp', 
                                                                        target='max_temp')})

operator.process(df)
