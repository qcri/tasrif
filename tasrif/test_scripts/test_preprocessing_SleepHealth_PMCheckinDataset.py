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
from tasrif.data_readers.sleep_health import PMCheckinDataset


pm = PMCheckinDataset(os.environ['SLEEPHEALTH_PMCHECKIN_PATH'])

print(pm.raw_dataframe())
print(pm.processed_dataframe())
print(pm.participant_count())
