# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: PyCharm (tasrif)
#     language: python
#     name: pycharm-5bd30262
# ---

# %%
# %load_ext autoreload
# %autoreload 2

import os
from tasrif.data_readers.sleep_health import AboutMeDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import DropNAOperator
from tasrif.processing_pipeline.pandas import DropDuplicatesOperator

# %%
# Full AboutMeDataset
aboutMe_full = AboutMeDataset(os.environ['SLEEPHEALTH_ABOUTME_PATH'], pipeline=None)
df_full = aboutMe_full.processed_dataframe()
print("Full Shape:", df_full.shape)

# %% pycharm={"name": "#%%\n"}
# Default AboutMeDataset
aboutMe_default = AboutMeDataset(os.environ['SLEEPHEALTH_ABOUTME_PATH'])
df_default = aboutMe_default.processed_dataframe()
print("Default Shape:", df_default.shape)

# %% pycharm={"name": "#%%\n"}
for key in df_full.keys():
    aboutMe_tmp = AboutMeDataset(os.environ['SLEEPHEALTH_ABOUTME_PATH'],
                             pipeline=SequenceOperator([
                                 DropNAOperator(subset=[key])
                             ])
                            )
    df_tmp = aboutMe_tmp.processed_dataframe()
    nNAs = df_full.shape[0] - df_tmp.shape[0]
    ntotal = df_full.shape[0]
    print("- ``%s`` has %d NAs (%d/%d = %.2f%%)" % (key, nNAs, nNAs, ntotal, 100.*nNAs/ntotal))

# %% pycharm={"name": "#%%\n"}
aboutMe_dropdup = AboutMeDataset(os.environ['SLEEPHEALTH_ABOUTME_PATH'],
                              pipeline=SequenceOperator([
                                DropDuplicatesOperator(subset='participantId',
                                                       keep='last')
                                  ])
                              )

print("Checking for duplicates?")
original = df_full.shape[0]
deduplicated = aboutMe_dropdup.processed_dataframe().shape[0]
print("Original size: %d, without duplicates %d. Dropped %d rows." % (original, deduplicated, original - deduplicated))

