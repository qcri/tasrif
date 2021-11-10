# +
import numpy as np
import pandas as pd
from tasrif.processing_pipeline import SequenceOperator, NoopOperator
from tasrif.processing_pipeline.custom import SetStartHourOfDayOperator
from tasrif.processing_pipeline.observers import VisualizeDaysObserver
from tasrif.processing_pipeline.pandas import FillNAOperator

def generate_days(periods, freq, participant=1, start_day="2018-01-01", name='startTime'):
    idx = pd.date_range(start_day, periods=periods, freq=freq, name='startTime')
    activity = np.random.randint(0, 100, periods)
    df = pd.DataFrame(data=activity, index=idx, columns=['activity'])
    df['steps'] = np.random.randint(100, 1000, periods)
    df['participant'] = participant
    return df

def generate_sleep(df, start_time='23:30', end_time='8:00', name='sleep'):
    df[name] = False
    time_filter = df.between_time(start_time=start_time, end_time=end_time).index
    df.loc[time_filter, name] = True
    df['not_' + name] = ~df[name]
    
    # reduce activity between 23:30 - 08:00
    df.loc[time_filter, 'activity'] = df.loc[time_filter, 'activity'] / 50
    df.loc[time_filter, 'steps'] = 0
    return df

def generate_data(participants=2, days=2):
    dfs = []
    for i in range(participants):
        df = generate_days(periods=24*days, freq='H', participant=i)
        df = generate_sleep(df) 
        dfs.append(df)
    return pd.concat(dfs)

df = generate_data()

# Add None to activity first day for participant 0
df.iloc[36:48, 0] = None
df

# +
# With no shift
observer = VisualizeDaysObserver(date_feature_name='startTime',
                                 signals=['activity', 'steps'],
                                 participant_identifier='participant',
                                 signals_as_area=['sleep'])

pipeline = SequenceOperator([NoopOperator()], observers=[observer])
pipeline.process(df)[0]

# +
# With shift
observer = VisualizeDaysObserver(date_feature_name='startTime',
                                 signals=['activity', 'steps'],
                                 participant_identifier='participant',
                                 signals_as_area=['sleep'],
                                 start_hour_col='shifted_time_col')


pipeline = SequenceOperator([
      SetStartHourOfDayOperator(date_feature_name='startTime',
                                participant_identifier='participant',
                                shifted_date_feature_name='shifted_time_col',
                                shift=6),
      FillNAOperator(value=300),
], observers=[observer])


pipeline.process(df)[0]
