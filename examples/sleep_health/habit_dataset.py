"""
Sleep Habit Dataset details can be found online at
``https://www.synapse.org/#!Synapse:syn18492837/wiki/593720``.

Some important stats:
    - Full Shape is (3303, 16)
    - This dataset contains 3303  rows.
    - ``participantId`` has 0 NAs ( 3303 / 3303 ) = 0.00 %
    - ``alarm_dependency`` has 17 NAs ( 3286 / 3303 ) = 0.51 %
    - ``driving_sleepy`` has 27 NAs ( 3276 / 3303 ) = 0.82 %
    - ``falling_asleep`` has 9 NAs ( 3294 / 3303 ) = 0.27 %
    - ``morning_person`` has 9 NAs ( 3294 / 3303 ) = 0.27 %
    - ``nap_duration`` has 1539 NAs ( 1764 / 3303 ) = 46.59 %
    - ``sleep_lost`` has 1495 NAs ( 1808 / 3303 ) = 45.26 %
    - ``sleep_needed`` has 49 NAs ( 3254 / 3303 ) = 1.48 %
    - ``sleep_partner`` has 8 NAs ( 3295 / 3303 ) = 0.24 %
    - ``sleep_time_workday`` has 15 NAs ( 3288 / 3303 ) = 0.45 %
    - ``sleep_time_weekend`` has 14 NAs ( 3289 / 3303 ) = 0.42 %
    - ``wake_up_choices`` has 7 NAs ( 3296 / 3303 ) = 0.21 %
    - ``wake_ups`` has 29 NAs ( 3274 / 3303 ) = 0.88 %
    - ``weekly_naps`` has 6 NAs ( 3297 / 3303 ) = 0.18 %
    - ``what_wakes_you`` has 13 NAs ( 3290 / 3303 ) = 0.39 %
    - ``timestamp`` has 0 NAs ( 3303 / 3303 ) = 0.00 %

The default pipeline:
    1. converts timestamp col to datatime;
    2. transforms into NA some values (i.e. answer for a question being "Don't known") in the following
       columns: "driving_sleepy", "morning_person", "nap_duration", "what_wakes_you".
    3. drop rows in which the number of NA is smaller than 5% of the rows.
        Accumulatively, this step removes 192 rows (5.8% of the original number of rows).
    4. One hot encode categorical features.

"""

import os
import numpy as np
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.sleep_health import SleepHealthDataset
from tasrif.processing_pipeline.pandas import DropNAOperator, ReplaceOperator, ConvertToDatetimeOperator
from tasrif.processing_pipeline.custom import OneHotEncoderOperator

shd_file_path = os.environ['SLEEPHEALTH_HABIT_PATH']

pipeline = ProcessingPipeline([
    SleepHealthDataset(shd_file_path),
    ConvertToDatetimeOperator(feature_names=["timestamp"],
                              format="%Y-%m-%dT%H:%M:%S",
                              utc=True),
    ReplaceOperator(
        to_replace={
            "driving_sleepy": {
                6: np.nan
            },
            "morning_person": {
                3: np.nan
            },
            "nap_duration": {
                6: np.nan
            },
            "what_wakes_you": {
                13: np.nan
            },
        }),
    DropNAOperator(subset=[
        "alarm_dependency",
        "driving_sleepy",
        "falling_asleep",
        "sleep_needed",
        "sleep_partner",
        "sleep_time_workday",
        "wake_up_choices",
        "wake_ups",
        "what_wakes_you",
    ]),
    OneHotEncoderOperator(feature_names=[
        "alarm_dependency",
        "driving_sleepy",
        "falling_asleep",
        "morning_person",
        "nap_duration",
        "sleep_partner",
        "wake_up_choices",
        "weekly_naps",
        "what_wakes_you",
    ]),
])
