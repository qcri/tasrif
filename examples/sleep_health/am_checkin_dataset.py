"""
Column description:
    - ``participant_id``  string
        Unique participant identification   n/a
    - ``AMCH1``   datetime
        Response to 'What time did you try to go to sleep?'
        (local time with timezone information)  n/a
    - ``AMCH2``     boolean
        Response to 'Did you have trouble falling asleep?'  TRUE/FALSE
    - ``AMCH2A\\*``   numeric
        Response to 'How long would you say it took you to fall asleep in minutes?' n/a
    - ``AMCH3\\*``    numeric
        Response to 'How many times did you wake up while sleeping
        (Do not include final awakening)'
    - ``AMCH3A\\*``   numeric
        Response to 'How long in total were you awake overnight?
        Enter total time you believe you were awake in minutes.' n/a
    - ``AMCH4``     datetime
        Response to 'What time did you wake up today?' (local time with timezone information)
    - ``AMCH-5\\*``   numeric
        Response to 'About how many minutes did you sleep last night?'  n/a
    - ``timestamp`` datetime
        Date & time of survey completion (local time with timezone information)


Some important stats:
 - This dataset contains unique data for  49480 participants.
 - ``participantId`` has 0 NAs ( 49480 / 49480 ) = 0.00 %
 - ``AMCH1`` has 549 NAs ( 48931 / 49480 ) = 1.11 %
 - ``AMCH2`` has 256 NAs ( 49224 / 49480 ) = 0.52 %
 - ``AMCH2A`` has 40201 NAs ( 9279 / 49480 ) = 81.25 %
 - ``AMCH3`` has 1353 NAs ( 48127 / 49480 ) = 2.73 %
 - ``AMCH3A`` has 13930 NAs ( 35550 / 49480 ) = 28.15 %
 - ``AMCH4`` has 377 NAs ( 49103 / 49480 ) = 0.76 %
 - ``AMCH5`` has 12761 NAs ( 36719 / 49480 ) = 25.79 %
 - ``timestamp`` has 0 NAs ( 49480 / 49480 ) = 0.00 %

"""

import os
import numpy as np
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.sleep_health import SleepHealthDataset
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    DropNAOperator,
    DropFeaturesOperator,
    ReplaceOperator,
    SortOperator,
)
from tasrif.processing_pipeline.custom import EncodeCyclicalFeaturesOperator

sleephealth_path = os.environ['SLEEPHEALTH']

pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "amcheckin"),
    ConvertToDatetimeOperator(feature_names=["AMCH1", "AMCH4"],
                              format="%Y-%m-%dT%H:%M:%S%z",
                              utc=True),
    SortOperator(by=["participantId"]),
    ReplaceOperator(
        to_replace={
            "AMCH2A": {
                np.nan: 0
            },
            "AMCH3A": {
                np.nan: 0
            },
            "AMCH5": {
                np.nan: 0
            },
        }),
    DropNAOperator(
        subset=["participantId", "AMCH1", "AMCH2", "AMCH3", "AMCH4"]),
    DropFeaturesOperator(['participantId', 'timestamp']),
    EncodeCyclicalFeaturesOperator(date_feature_name='AMCH4',
                                   category_definition=[
                                       "month", "day_in_month", "day",
                                       "hour", "minute"
                                   ])
])


df = pipeline.process()

print(df)
