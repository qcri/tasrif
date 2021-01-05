#!/usr/bin/env python
# coding: utf-8

# # Zenodo - preprocessing

# # Preprocessing

# In[3]:


import pandas as pd
import numpy as np
import pathlib
import datetime
from tasrif.data_readers.zenodo_fitbit_dataset import (
    ZenodoFitbitActivityDataset, 
    ZenodoFitbitWeightDataset, 
    ZenodoFitbitSleepDataset, 
    ZenodoCompositeFitbitDataset)

nan = np.nan
zenodo_folder = '~/Documents/Data/Zenodo'

zfd = ZenodoFitbitActivityDataset(zenodo_folder=zenodo_folder)
df = zfd.processed_dataframe()
adf = zfd.grouped_dataframe()
pdfs = zfd.participant_dataframes()

zwd = ZenodoFitbitWeightDataset(zenodo_folder=zenodo_folder)
zwd.raw_dataframe()
zwd.processed_dataframe()
wdf = zwd.grouped_dataframe()

zsd = ZenodoFitbitSleepDataset(zenodo_folder=zenodo_folder)
df = zsd.processed_dataframe()
sdf = zsd.grouped_dataframe()
#[pd.DataFrame(y) for x, y in df.groupby('logId', as_index=False)]


# In[4]:


adf


# In[5]:


zfd.participant_count()


# In[4]:


sdf


# In[5]:


cds = ZenodoCompositeFitbitDataset([zfd, zwd, zsd])
df = cds.grouped_dataframe()
df


# In[6]:


df = df.dropna(axis=1)
df = df.drop(['sleep_episodes_count', 'Id'], axis=1)
df


# In[ ]:





# In[7]:


from sihatk.dimensionality_reduction.dimensionality_reduction import identify_confounding_variables


# In[8]:


identify_confounding_variables(df)


# In[12]:


corr_df = df.corr()


# In[10]:


corr_df


# In[ ]:




