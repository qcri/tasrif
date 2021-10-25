"""
Column description:
    - ``participant_id``  string
        Unique participant identification   n/a
    - ``alcohol*``   numeric
        Response to 'How many alcoholic beverages did you consume today?'
    - ``caffeine\\*``     numeric
        Response to 'How many caffeinated beverages did you consume today?'
    - ``NapCount``   categorical
        Response to 'How many times did you nap or doze today?'
        0=None,1=Once,2=Twice,3=Three times of more
    - ``PMCH1``    categorical
        Response to 'How difficult was it for you to stay awake today?'
        1=Very difficult,2=Somewhat difficult,3=Not difficult
    - ``PMCH2A\\*``   numeric
            Response to 'In total, how long did you nap or doze today?
            (Enter estimated time in minutes)'
    - ``PMCH3``     categorical
        Response to 'Did you consume any of the folowing today?'
        0=None of the above,1=Caffeine,10=Alcohol,100=Medication
    - ``timestamp`` datetime
        Date & time of survey completion (local time with timezone information)


Some important stats:
    - This dataset contains unique data for  27380 participants.
    - ``participantId`` has 0 NAs ( 27380 / 27380 ) = 0.00 %
    - ``NapCount`` has 125 NAs ( 27255 / 27380 ) = 0.46 %
    - ``PMCH1`` has 134 NAs ( 27246 / 27380 ) = 0.49 %
    - ``PMCH2A`` has 19942 NAs ( 7438 / 27380 ) = 72.83 %
    - ``PMCH3`` has 192 NAs ( 27188 / 27380 ) = 0.70 %
    - ``timestamp`` has 0 NAs ( 27380 / 27380 ) = 0.00 %
    - ``alcohol`` has 0 NAs ( 27380 / 27380 ) = 0.00 %
    - ``caffeine`` has 0 NAs ( 27380 / 27380 ) = 0.00 %

"""

import os
import numpy as np
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.sleep_health import SleepHealthDataset
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    DropFeaturesOperator,
    ReplaceOperator,
    SortOperator,
)
from tasrif.processing_pipeline.custom import OneHotEncoderOperator

sleephealth_path = os.environ['SLEEPHEALTH']

pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "pmcheckin"),
    ConvertToDatetimeOperator(feature_names=["timestamp"],
                              format="%Y-%m-%dT%H:%M:%S%z",
                              utc=True),
    SortOperator(by=["participantId", "timestamp"]),
    ReplaceOperator(to_replace={
        "PMCH2A": {
            np.nan: 0
        },
        "PMCH3": {
            np.nan: 0
        },
        "PMCH1": {
            np.nan: 3
        },
        "NapCount": {
            np.nan: 0
        },
    }),
    DropFeaturesOperator(['participantId', 'timestamp']),
    OneHotEncoderOperator(feature_names=['NapCount', 'PMCH1', 'PMCH3'],
                          drop_first=False)
])


df = pipeline.process()

print(df)
