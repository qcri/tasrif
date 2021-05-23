# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import pandas as pd
from tasrif.processing_pipeline.custom import FlagEpochActivityLessThanOperator
from tasrif.processing_pipeline.custom import FlagDayIfValidEpochsSmallerThanOperator
from tasrif.processing_pipeline.custom import FlagDayIfValidEpochsLargerThanOperator
from tasrif.processing_pipeline.custom import FlagEpochNullColsOperator
from tasrif.processing_pipeline.custom import FlagDayIfNotEnoughConsecutiveDaysOperator
from tasrif.processing_pipeline.custom import ValidationReportOperator
from tasrif.processing_pipeline.custom import RemoveFlaggedDaysOperator

from tasrif.data_readers.my_heart_counts import HealthKitDataDataset
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator
from tasrif.processing_pipeline.custom import IterateCsvOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator
from tasrif.processing_pipeline import ProcessingPipeline

df = pd.DataFrame({'id': [1, 2, 3], 'activity': [100, 3, 57]})

validator_pipeline = ProcessingPipeline([FlagEpochActivityLessThanOperator(activity_col='value', min_activity_threshold=5),
                                        FlagDayIfValidEpochsSmallerThanOperator(valid_minutes_per_day=5),
                                        FlagDayIfValidEpochsLargerThanOperator(max_invalid_minutes_per_day=5),
                                        FlagEpochNullColsOperator(col_list=['value']),
                                        ])


CSV_FOLDER = '/mnt/c/Development/projects/siha/HealthKitData_timeseries'
CSV_PIPELINE = ProcessingPipeline([ConvertToDatetimeOperator(feature_names=["startTime", "endTime"], utc=True)])

PIPELINE = ProcessingPipeline([CreateFeatureOperator(feature_name='file_name', feature_creator=lambda df: df['recordId'] + '.csv'),
                               IterateCsvOperator(folder_path=CSV_FOLDER, field='file_name', pipeline=CSV_PIPELINE)
                               ])

hkd = HealthKitDataDataset(mhc_folder='/mnt/c/Development/projects/siha', processing_pipeline=PIPELINE)
record, df = next(hkd.processed_df)
# -

hkd.raw_df

record

df

validator_pipeline.process(df)[0]

RemoveFlaggedDaysOperator().process(df)[0]




