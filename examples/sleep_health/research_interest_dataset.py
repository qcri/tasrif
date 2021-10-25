"""
Research Interest details can be found online at``https://www.synapse.org/#!Synapse:syn18492837/wiki/593711``.

Some important stats:
    - Original Shape is (2359, 9)
    - This dataset contains 1551 rows for 1478 unique participants.
    - ``participantId`` has 0 NAs ( 2359 / 2359 ) = 0.00 %
    - ``contact_method`` has 8 NAs ( 2351 / 2359 ) = 0.34 %
    - ``research_experience`` has 4 NAs ( 2355 / 2359 ) = 0.17 %
    - ``two_surveys_perday`` has 8 NAs ( 2351 / 2359 ) = 0.34 %
    - ``blood_sample`` has 6 NAs ( 2353 / 2359 ) = 0.25 %
    - ``taking_medication`` has 7 NAs ( 2352 / 2359 ) = 0.30 %
    - ``family_survey`` has 10 NAs ( 2349 / 2359 ) = 0.42 %
    - ``hospital_stay`` has 11 NAs ( 2348 / 2359 ) = 0.47 %
    - ``timestamp`` has 0 NAs ( 2359 / 2359 ) = 0.00 %

The default pipeline:
    1. converts timestamp to datatime, sort the dataframe by time and removes all duplicates entries for the
       same participant id, retaining only the last one;
    2. transforms into NA some values (i.e. answer for a question being "Don't known") in all columns;
    3. drop NA rows after the transformation above for all columns;
    4. One hot encode categorical features.

Final dataset shape after default preprocessing pipeline: (2072, 21)

"""


import os
import numpy as np
from tasrif.processing_pipeline import SequenceOperator
from tasrif.data_readers.sleep_health import SleepHealthDataset
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    DropNAOperator,
    DropDuplicatesOperator,
    ReplaceOperator,
    SortOperator
)
from tasrif.processing_pipeline.custom import OneHotEncoderOperator

sleephealth_path = os.environ['SLEEPHEALTH']

pipeline = SequenceOperator([
    SleepHealthDataset(sleephealth_path, "researchinterest"),
    ConvertToDatetimeOperator(feature_names="timestamp",
                              format="%Y-%m-%dT%H:%M:%S%z",
                              utc=True),
    SortOperator(by=["participantId", "timestamp"]),
    DropDuplicatesOperator(subset="participantId", keep="last"),
    ReplaceOperator(to_replace={"research_experience": {
        3: np.nan
    }}),
    DropNAOperator(subset=[
        "contact_method",
        "research_experience",
        "two_surveys_perday",
        "blood_sample",
        "taking_medication",
        "family_survey",
        "hospital_stay",
    ]),
    OneHotEncoderOperator(
        feature_names=[
            "contact_method",
            "research_experience",
            "two_surveys_perday",
            "blood_sample",
            "taking_medication",
            "family_survey",
            "hospital_stay",
        ],
        drop_first=True,
    ),
])

df = pipeline.process()

print(df)
