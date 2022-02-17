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

import numpy as np

# %%
import pandas as pd

from tasrif.processing_pipeline.custom import AggregateOperator, LinearFitOperator

df = pd.DataFrame([['001', 25, 30], ['001', 17, 50], ['002', 20, 40], ['002', 21, 42]],
                     columns=['pid', 'min_activity', 'max_activity'])
# %%
operator = AggregateOperator(groupby_feature_names ="pid",
                             aggregation_definition= {"min_activity": ["mean", "std"],
                                                      "r2,_,intercept": LinearFitOperator(feature_names='min_activity', 
                                                                        target='max_activity')})

operator.process(df)
