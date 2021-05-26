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
import os
import pandas as pd
from tasrif.data_readers.sleep_health import AMCheckinDataset


am = AMCheckinDataset(os.environ['SLEEPHEALTH_AMCHECKIN_PATH'])

print(am.raw_dataframe())
print(am.processed_dataframe())
print(am.participant_count())
