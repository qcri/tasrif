# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2
import os
from tasrif.data_readers.my_heart_counts import MyHeartCountsDataset

# %% pycharm={"name": "#%%\n"}
mhc_file_path = os.environ['MYHEARTCOUNTS']
aas = MyHeartCountsDataset(mhc_file_path, "activitysleepsurvey")
aas.process()[0]

# %% pycharm={"name": "#%%\n"}
cds = MyHeartCountsDataset(mhc_file_path, "cardiodietsurvey")
cds.process()[0]

# %% pycharm={"name": "#%%\n"}
dcs = MyHeartCountsDataset(mhc_file_path, "dailychecksurvey")
dcs.process()[0]

# %% pycharm={"name": "#%%\n"}
dcs = MyHeartCountsDataset(mhc_file_path, "dayonesurvey")
dcs.process()[0]

# %% pycharm={"name": "#%%\n"}
dmo = MyHeartCountsDataset(mhc_file_path, "demographics")
dmo.process()[0]

# %% pycharm={"name": "#%%\n"}
hkd = MyHeartCountsDataset(mhc_file_path, "healthkitdata")
hkd.process()[0]

# %% pycharm={"name": "#%%\n"}
has = MyHeartCountsDataset(mhc_file_path, "heartagesurvey")
has.process()[0]

# %% pycharm={"name": "#%%\n"}
parq = MyHeartCountsDataset(mhc_file_path, "parqsurvey")
parq.process()[0]

# %% pycharm={"name": "#%%\n"}
qol = MyHeartCountsDataset(mhc_file_path)
qol.process()[0]

# %% pycharm={"name": "#%%\n"}
rf = MyHeartCountsDataset(mhc_file_path, "qualityoflife")
rf.process()[0]

# %% pycharm={"name": "#%%\n"}
smwa = MyHeartCountsDataset(mhc_file_path, "sixminutewalkactivity")
smwa.process()[0]

# %% pycharm={"name": "#%%\n"}
rfs = MyHeartCountsDataset(mhc_file_path, "riskfactorsurvey")
rfs.process()[0]
