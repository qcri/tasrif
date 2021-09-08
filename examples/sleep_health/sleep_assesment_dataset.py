"""
Sleep Assessment Dataset details can be found online at
``https://www.synapse.org/#!Synapse:syn18492837/wiki/593721``.

Some important stats:
    - Original Shape is (2325, 23)
    - This dataset contains 2325 rows for 2228 unique participants.
    - ``participantId`` has 0 NAs ( 2325 / 2325 ) = 0.00 %
    - ``alcohol`` has 2 NAs ( 2323 / 2325 ) = 0.09 %
    - ``concentrating_problem_one`` has 4 NAs ( 2321 / 2325 ) = 0.17 %
    - ``concentrating_problem_two`` has 4 NAs ( 2321 / 2325 ) = 0.17 %
    - ``discomfort_in_sleep`` has 7 NAs ( 2318 / 2325 ) = 0.30 %
    - ``exercise`` has 30 NAs ( 2295 / 2325 ) = 1.29 %
    - ``fatigue_limit`` has 9 NAs ( 2316 / 2325 ) = 0.39 %
    - ``feel_tired_frequency`` has 1 NAs ( 2324 / 2325 ) = 0.04 %
    - ``felt_alert`` has 1 NAs ( 2324 / 2325 ) = 0.04 %
    - ``had_problem`` has 6 NAs ( 2319 / 2325 ) = 0.26 %
    - ``hard_times`` has 3 NAs ( 2322 / 2325 ) = 0.13 %
    - ``medication_by_doctor`` has 4 NAs ( 2321 / 2325 ) = 0.17 %
    - ``poor_sleep_problems`` has 0 NAs ( 2325 / 2325 ) = 0.00 %
    - ``sleep_aids`` has 4 NAs ( 2321 / 2325 ) = 0.17 %
    - ``sleep_problem`` has 8 NAs ( 2317 / 2325 ) = 0.34 %
    - ``think_clearly`` has 3 NAs ( 2322 / 2325 ) = 0.13 %
    - ``tired_easily`` has 2 NAs ( 2323 / 2325 ) = 0.09 %
    - ``told_by_doctor`` has 9 NAs ( 2316 / 2325 ) = 0.39 %
    - ``told_by_doctor_specify`` has 1642 NAs ( 683 / 2325 ) = 70.62 %
    - ``told_to_doctor`` has 1 NAs ( 2324 / 2325 ) = 0.04 %
    - ``other_selected`` has 2249 NAs ( 76 / 2325 ) = 96.73 %
    - ``trouble_staying_awake`` has 3 NAs ( 2322 / 2325 ) = 0.13 %
    - ``timestamp`` has 0 NAs ( 2325 / 2325 ) = 0.00 %

The default pipeline:
    (1) converts timestamp to datatime, sort the dataframe by time and removes all duplicates entries for the
        same participant id, retaining only the last one;
    (2) transforms into NA some values (i.e. answer for a question being "Don't known") in all columns;
    (3) drop NA rows after the transformation above for all columns;
    (4) One hot encode categorical features;
    (5) Dataset contains one string column ('told_by_doctor_specify'), with text that was not preprocessed.

Final dataset shape after default preprocessing pipeline: (2123, 83)

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

sa_file_path = os.environ['SLEEPHEALTH_SLEEP_ASSESMENT_PATH']

pipeline = SequenceOperator([
    SleepHealthDataset(sa_file_path),
    ConvertToDatetimeOperator(feature_names="timestamp",
                              format="%Y-%m-%dT%H:%M:%S%z",
                              utc=True),
    SortOperator(by=["participantId", "timestamp"]),
    DropDuplicatesOperator(subset="participantId", keep="last"),
    ReplaceOperator(
        to_replace={
            "alcohol": {
                7: np.nan
            },
            "medication_by_doctor": {
                7: np.nan
            },
            "sleep_aids": {
                7: np.nan
            },
            "told_by_doctor": {
                3: np.nan
            },
            "told_to_doctor": {
                3: np.nan
            },
            "told_by_doctor_specify": {
                np.nan: "8"
            },
            "other_selected": {
                np.nan: ""
            },
        }),
    DropNAOperator(subset=[
        "alcohol",
        "concentrating_problem_one",
        "concentrating_problem_two",
        "discomfort_in_sleep",
        "exercise",
        "fatigue_limit",
        "feel_tired_frequency",
        "felt_alert",
        "had_problem",
        "hard_times",
        "medication_by_doctor",
        "poor_sleep_problems",
        "sleep_aids",
        "sleep_problem",
        "think_clearly",
        "tired_easily",
        "told_by_doctor",
        "told_to_doctor",
        "trouble_staying_awake",
    ]),
    OneHotEncoderOperator(
        feature_names=[
            "alcohol",
            "concentrating_problem_one",
            "concentrating_problem_two",
            "discomfort_in_sleep",
            "exercise",
            "fatigue_limit",
            "feel_tired_frequency",
            "felt_alert",
            "had_problem",
            "hard_times",
            "medication_by_doctor",
            "poor_sleep_problems",
            "sleep_aids",
            "sleep_problem",
            "think_clearly",
            "tired_easily",
            "told_by_doctor",
            "told_to_doctor",
            "trouble_staying_awake",
            "told_by_doctor_specify",
        ],
        drop_first=True,
    ),
])


df = pipeline.process()

print(df)
