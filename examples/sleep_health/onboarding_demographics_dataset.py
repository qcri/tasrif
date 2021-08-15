"""
Sleep Quality Checker Dataset details can be found online at
``https://www.synapse.org/#!Synapse:syn18492837/wiki/590798``.
According to the documentation, Height values <60in or >78in and weight values
<80 or >350 have been censored to protect participants with potentially identifying features.

The default pipeline works like that:
    1. replaces the "CENSORED" values presented in this dataset by NAs.
    2. Drops NAs:
        2.(a) gender: 89 (1.09%)
        2.(b) age_years: 54 (0.66%)
        2.(c) weight_pounds: 431 (5.3%)
        2.(d) height_inches: 337 (4.1%)

The final dataset size after removing all NAs is 7558 (retaining 93% of the original dataset).

"""


import os
import numpy as np
from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.data_readers.sleep_health import SleepHealthDataset
from tasrif.processing_pipeline.pandas import DropNAOperator, ReplaceOperator

obd_file_path = os.environ['SLEEPHEALTH_ONBOARDING_DEMOGRAPHICS_PATH']

pipeline = ProcessingPipeline([
    SleepHealthDataset(obd_file_path),
    ReplaceOperator(to_replace="CENSORED", value=np.nan),
    DropNAOperator()
])

df = pipeline.process()

print(df)
