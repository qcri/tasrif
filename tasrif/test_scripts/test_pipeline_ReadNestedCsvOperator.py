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

# %% [markdown]
# # The following code requires two small csv files to be saved in disk in order to test ReadNestedCsvOperator

# %%
import pandas as pd
import numpy as np

from tasrif.processing_pipeline.custom import ReadNestedCsvOperator

df = pd.DataFrame({"name": ['Alfred', 'Roy'],
                   "age": [43, 32],
                   "file_details": ['details1.csv', 'details2.csv']})

details1 = pd.DataFrame({'calories': [360, 540],
                         'time': [pd.Timestamp("2015-04-25"), pd.Timestamp("2015-04-26")]
                        })

details2 = pd.DataFrame({'calories': [420, 250],
                         'time': [pd.Timestamp("2015-05-16"), pd.Timestamp("2015-05-17")]
                        })


# Save File 1 and File 2
details1.to_csv('details1.csv', index=False)
details2.to_csv('details2.csv', index=False)

operator = ReadNestedCsvOperator(folder_path='./', field='file_details', pipeline=None)
generator = operator.process(df)[0]

# Iterates twice
for record, details in generator:
    print('Subject information:')
    print(record)
    print('')
    print('Subject details:')
    print(details)
    print('============================')
