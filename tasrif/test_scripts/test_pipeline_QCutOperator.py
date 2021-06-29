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
import numpy as np
import datetime

from tasrif.processing_pipeline.pandas import QCutOperator


df = pd.DataFrame({
        'Time': pd.date_range('2018-01-01', '2018-01-10', freq='1H', closed='left'),
        'Steps': np.random.randint(100,5000, size=9*24),
        }
     )

ids = []
for i in range(1, 217):
    ids.append(i%10 + 1)
    
df["Id"] = ids
# -

df

# +
# 4 Equally distributed bins
df1 = df.copy()
operator = QCutOperator(cut_column_name='Steps', 
                       bin_column_name='Bin',
                       quantile=4,
                       retbins=True)

df1, bins = operator.process(df1)[0]
print('Bins:', bins)
print(df1['Bin'].value_counts())
df1

# +
# Custom bins
cut_labels = ['Sedentary', "Light", 'Moderate', 'Vigorous']
quantiles = [0, 0.2, 0.5, 0.80, 1]

df2 = df.copy()
operator = QCutOperator(cut_column_name='Steps', 
                        bin_column_name='Bin',
                        quantile=quantiles,
                        labels=cut_labels)

df2 = operator.process(df1)[0]
print(df2['Bin'].value_counts())
df2
