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
import os
import pandas as pd
import datetime

import numpy as np
from tasrif.data_readers.zenodo_fitbit_dataset import (
    ZenodoFitbitIntradayCaloriesDataset,
    ZenodoFitbitIntradayIntensitiesDataset,
    ZenodoFitbitIntradayMETsDataset,
    ZenodoFitbitIntradayStepsDataset,
    ZenodoFitbitSleepDataset,
)

zenodo_folder = os.environ['ZENODOFITBIT_PATH']

# %%
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

# %%
zsd = ZenodoFitbitIntradayStepsDataset(zenodo_folder=zenodo_folder)
steps_dataset = zsd.processed_dataframe()

# Splitting date and hour of day
steps_dataset['date'] = [d.date() for d in steps_dataset['ActivityMinute']]
steps_dataset['time'] = [d.time() for d in steps_dataset['ActivityMinute']]

# Applying pivot to make each hour of day a column

steps_dataset = steps_dataset.pivot_table('Steps', ['date', 'Id'], 'time')

steps_dataset.head()

# %%
zsld = ZenodoFitbitSleepDataset(zenodo_folder=zenodo_folder)
sleep_dataset = zsld.processed_dataframe()

# %%
# Extracting date from first sleep datetime and counting sleep after 6 am as sleep of current day else previous day
sleep_dataset['date'] = [d.date() if (int(d.time().strftime("%H")) > 6) else (d.date() - datetime.timedelta(days=1)) for d in sleep_dataset['date_first']]

sleep_dataset

# %%
combined_dataset = pd.merge(steps_dataset, sleep_dataset, how='left', left_on=['Id','date'], right_on=['Id','date'])
combined_dataset.dropna(subset = ["total_sleep_seconds"], inplace=True) # Making sure we have y values

for col in combined_dataset.columns:
    if(type(col) == datetime.time):
        combined_dataset = combined_dataset.rename(columns={col: col.strftime("%H")})

# Selecting relevant times of day to evaluate sleep - 7 am to 11 pm
non_hour_columns = ['Id', 'date', 'logId', 'duration_sum', 'date_first' ,'value_mean', 'total_sleep_seconds',
                    '00', '01', '02', '03', '04', '05', '06']
hour_columns = [ col for col in combined_dataset.columns if col not in non_hour_columns ]
for column in hour_columns:
    # Removing NA values from any of the x values
    combined_dataset[column] = combined_dataset[column].fillna(0)

combined_dataset

# %%
# Starting multiple regression

# Defining x and y

dataset_x = combined_dataset.drop(non_hour_columns, axis=1).values

dataset_y = combined_dataset['total_sleep_seconds'].values

dataset_x

# %%
# Normalizing X values

from sklearn.preprocessing import MinMaxScaler

transformer_x = MinMaxScaler().fit(dataset_x)

dataset_x = transformer_x.transform(dataset_x)

# To inverse the transformation
# dataset_x = transformer_x.inverse_transform(dataset_x)

# Normalizing y values 

norm_y = np.linalg.norm(dataset_y)

dataset_y = dataset_y/norm_y

# %%
# Split the data in training and testing set

# from sklearn.model_selection import train_test_split
# x_train, x_test, y_train, y_test = train_test_split(dataset_x, dataset_y, test_size=0.2, random_state=0)

# %%
# Train model on the training set

from sklearn.linear_model import LinearRegression
lr_model = LinearRegression()
# lr_fit = lr_model.fit(x_train, y_train)

# Perform cross validation test instead of split

from sklearn.model_selection import cross_validate

cross_val_scores = cross_validate(lr_model, dataset_x, dataset_y, cv=5, return_estimator=True)
cross_val_scores

# %%
# x_train.max()

# %%
# lr_fit.coef_

for model in cross_val_scores['estimator']:
    print(model.coef_)

# %%
for model in cross_val_scores['estimator']:   
    coef_df = pd.DataFrame(model.coef_, columns=['Coefficient']) # replace model with lr_fit if splitting
    coef_df['Hour'] = hour_columns
    coef_df
    coef_df.plot(x="Hour", y=["Coefficient"], kind="bar")

# %%
# lr_fit.intercept_

for model in cross_val_scores['estimator']:
    print(model.intercept_)

# %%
# Evaluate the model

# from sklearn.metrics import r2_score, mean_squared_error

# y_pred = lr_model.predict(x_test)
# r2_score(y_test, y_pred)
# mean_squared_error(y_test, y_pred)
