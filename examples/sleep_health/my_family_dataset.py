"""
My Family Dataset details can be found online at``https://www.synapse.org/#!Synapse:syn18492837/wiki/593712``.

Some important stats:
    - Original Shape (3003, 6)
    - This dataset contains 3003 rows for 2760 unique participants.
    - ``participantId`` has 0 NAs ( 3003 / 3003 ) = 0.00 %
    - ``fam_history`` has 15 NAs ( 2988 / 3003 ) = 0.50 %
    - ``family_size`` has 7 NAs ( 2996 / 3003 ) = 0.23 %
    - ``language`` has 4 NAs ( 2999 / 3003 ) = 0.13 %
    - ``underage_family`` has 9 NAs ( 2994 / 3003 ) = 0.30 %
    - ``timestamp`` has 0 NAs ( 3003 / 3003 ) = 0.00 %

The default pipeline:
    1. converts timestamp to datatime, sort the dataframe by time and removes all duplicates entries for the
       same participant id, retaining only the last one;
    2. transforms into NA some values (i.e. answer for a question being "Don't known") in all columns;
    3. drop NA rows after the transformation above for all columns;
    4. One hot encode categorical features.
    5. Drop remaining col for fam_history=200 ("Prefer not to answer"). Apparently some users answered the
       questionnaire AND included the option "prefer not to answer" as well.

Final dataset shape after default preprocessing pipeline: (2695, 21)

"""

import os

import numpy as np

from tasrif.data_readers.sleep_health import SleepHealthDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.custom import OneHotEncoderOperator
from tasrif.processing_pipeline.pandas import (
    ConvertToDatetimeOperator,
    DropDuplicatesOperator,
    DropFeaturesOperator,
    DropNAOperator,
    ReplaceOperator,
    SortOperator,
)

sleephealth_path = os.environ["SLEEPHEALTH"]

pipeline = SequenceOperator(
    [
        SleepHealthDataset(sleephealth_path, "myfamily"),
        ConvertToDatetimeOperator(
            feature_names="timestamp", format="%Y-%m-%dT%H:%M:%S%z", utc=True
        ),
        SortOperator(by=["participantId", "timestamp"]),
        DropDuplicatesOperator(subset="participantId", keep="last"),
        ReplaceOperator(
            to_replace={
                "fam_history": {"200": np.nan},
                "family_size": {6: np.nan},
                "language": {5: np.nan},
                "underage_family": {6: np.nan},
            }
        ),
        DropNAOperator(
            subset=["fam_history", "family_size", "language", "underage_family"]
        ),
        OneHotEncoderOperator(
            feature_names=[
                "fam_history",
                "family_size",
                "language",
                "underage_family",
            ],
            drop_first=False,
        ),
        DropFeaturesOperator(["fam_history=200"]),
    ]
)

df = pipeline.process()

print(df)
