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

from tasrif.processing_pipeline.custom import CreateFeatureOperator
df0 = pd.DataFrame([['tom', 10, 2], ['nick', 15, 2], ['juli', 14, 12]],
                    columns=['name', 'work_hours', 'off_hours'])
print(df0)
operator = CreateFeatureOperator(
   feature_name="total_hours",
   feature_creator=lambda df: df['work_hours'] + df['off_hours'])
df0 = operator.process(df0)
print(df0)
