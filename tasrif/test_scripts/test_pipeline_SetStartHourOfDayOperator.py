# +
import numpy as np
import pandas as pd

from tasrif.processing_pipeline.custom import SetStartHourOfDayOperator

# Prepare two days for two participants data
four_days = 48*2
idx = pd.date_range("2018-01-01", periods=four_days, freq="H", name='startTime')
activity = np.random.randint(0, 100, four_days)
df = pd.DataFrame(data=activity, index=idx, columns=['activity'])
df['participant'] = 1
df.iloc[48:, 1] = 2


operator = SetStartHourOfDayOperator(date_feature_name='startTime',
                                     participant_identifier='participant',
                                     shift=6)
df = df.reset_index()
operator.process(df)[0]
