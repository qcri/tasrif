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

# %% [markdown]
# # In order to test ReadCsvFolderOperator, the following is required:
# - Two small csv files to be saved in disk in order to test 

# %%
import pandas as pd
import numpy as np

from tasrif.processing_pipeline.custom import ReadCsvFolderOperator
from tasrif.processing_pipeline.pandas import ConcatOperator
from tasrif.processing_pipeline import SequenceOperator


details1 = pd.DataFrame({'calories': [360, 540],
                         'time': [pd.Timestamp("2015-04-25"), pd.Timestamp("2015-04-26")]
                        })

details2 = pd.DataFrame({'calories': [420, 250],
                         'time': [pd.Timestamp("2015-05-16"), pd.Timestamp("2015-05-17")]
                        })
 

# Save File 1 and File 2
details1.to_csv('./details1.csv', index=False)
details2.to_csv('./details2.csv', index=False)

pipeline = SequenceOperator([
    ReadCsvFolderOperator(name_pattern='./*.csv', pipeline=None),
])

df = pipeline.process()[0]
df

# %%
