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

import numpy as np
from tasrif.data_readers.zenodo_fitbit_dataset import (
    ZenodoFitbitIntradayCaloriesDataset,
    ZenodoFitbitIntradayIntensitiesDataset,
    ZenodoFitbitIntradayMETsDataset,
    ZenodoFitbitIntradayStepsDataset,
)

zenodo_folder = os.environ['ZENODOFITBIT_PATH']
zcd = ZenodoFitbitIntradayCaloriesDataset(zenodo_folder=zenodo_folder)
print(zcd.processed_dataframe())
print(zcd.grouped_dataframe())

# %%
zid = ZenodoFitbitIntradayIntensitiesDataset(zenodo_folder=zenodo_folder)
print(zid.processed_dataframe())
print(zid.grouped_dataframe())

# %%
zmetd = ZenodoFitbitIntradayMETsDataset(zenodo_folder=zenodo_folder)
print(zmetd.processed_dataframe())
print(zmetd.grouped_dataframe())

# %%
zsd = ZenodoFitbitIntradayStepsDataset(zenodo_folder=zenodo_folder)
print(zsd.processed_dataframe())
print(zsd.grouped_dataframe())
