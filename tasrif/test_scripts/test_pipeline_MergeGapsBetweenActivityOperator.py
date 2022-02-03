# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
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

import pandas as pd
import numpy as np
from tasrif.processing_pipeline.custom import MergeGapsBetweenActivityOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator

# +
df = pd.DataFrame([
    [0,2,354,27,5,386,0.91,'2016-03-27 03:33:00', '2016-03-27 09:02:00'],
    [0,4,312,23,7,321,0.93,'2016-03-28 00:40:00', '2016-03-28 01:56:00'],
    [0,5,312,35,7,193,0.93,'2016-03-29 00:40:00', '2016-03-29 01:56:00'],
    [0,7,312,52,7,200,0.93,'2016-05-21 00:40:00', '2016-05-21 01:56:00'],
    [0,8,312,12,7,43,0.93,'2016-05-23 01:57:00', '2016-05-23 01:58:00'],
    [0,9,312,42,7,100,0.93,'2016-05-23 01:59:00', '2016-05-23 01:59:30'],
    [0,9,312,21,7,302,0.93,'2016-05-23 03:00:00', '2016-05-23 03:59:30'],
    [0,10,312,16,7,335,0.93,'2016-05-23 10:57:00', '2016-05-23 20:58:00'],
    [0,11,312,16,7,335,0.93,'2016-10-24 00:58:00', '2016-05-24 01:58:00'],
    [1,3,312,16,7,335,0.93,"2016-03-14 08:12:00","2016-03-14 10:15:00"],
    [1,4,272,26,5,303,0.89,"2016-03-16 03:12:00","2016-03-16 08:14:00"],
    [1,5,61,2,0,63,0.96,"2016-03-16 19:43:00","2016-03-16 20:45:00"],
    [1,6,402,34,1,437,0.91,"2016-03-17 01:16:00","2016-03-17 08:32:00"],
],
    columns=["Id",
           "logId",
           "Total Minutes Asleep",
           "Total Minutes Restless",
           "Total Minutes Awake",
           "Total Minutes in Bed",
           "Sleep Efficiency",
           "Sleep Start",
           "Sleep End"])

df['Sleep Start'] = pd.to_datetime(df['Sleep Start'])
df['Sleep End'] = pd.to_datetime(df['Sleep End'])
# -


df

# +


aggregation_definition = {
    'logId': lambda df: df.iloc[0],
    'Total Minutes Asleep': np.sum,
    'Total Minutes Restless': np.sum,
    'Total Minutes Awake': np.sum,
    'Total Minutes in Bed': np.sum,
}

operator = MergeGapsBetweenActivityOperator(
                                participant_identifier='Id',
                                start_date_feature_name='Sleep Start',
                                end_date_feature_name='Sleep End',
                                threshold="3 hour",
                                aggregation_definition=aggregation_definition)
df = operator.process(df)[0]
df

# +
# Test cases: 
# seq1: a sequence of [0, 0] no merge
# seq2: a sequence of [1, 0] no merge
# seq3: a sequence of [0, 1] merge
# seq4: a sequence of [1, 1] no merge

operator = MergeGapsBetweenActivityOperator(
                                participant_identifier='Id',
                                start_date_feature_name='Sleep Start',
                                end_date_feature_name='Sleep End',
                                threshold="3 hour",
                                aggregation_definition=aggregation_definition,
                                return_before_merging=False)

seq1 = pd.DataFrame([
    [0, '2016-03-27 03:33:00', '2016-03-27 03:34:00'],
    [0, '2016-03-27 03:34:00', '2016-03-28 03:54:00'],
], columns=["Id", "Sleep Start", "Sleep End"])
seq1['Sleep Start'] = pd.to_datetime(seq1['Sleep Start'])
seq1['Sleep End'] = pd.to_datetime(seq1['Sleep End'])

seq2 = pd.DataFrame([
    [0, '2016-03-27 03:33:00', '2016-03-27 09:02:00'],
    [0, '2016-03-27 09:40:00', '2016-03-28 10:30:00'],
], columns=["Id", "Sleep Start", "Sleep End"])
seq2['Sleep Start'] = pd.to_datetime(seq2['Sleep Start'])
seq2['Sleep End'] = pd.to_datetime(seq2['Sleep End'])

seq3 = pd.DataFrame([
    [0, '2016-03-27 03:33:00', '2016-03-27 03:50:00'],
    [0, '2016-03-27 03:55:00', '2016-03-28 10:30:00'],
], columns=["Id", "Sleep Start", "Sleep End"])
seq3['Sleep Start'] = pd.to_datetime(seq3['Sleep Start'])
seq3['Sleep End'] = pd.to_datetime(seq3['Sleep End'])

seq4 = pd.DataFrame([
    [0, '2016-03-27 03:33:00', '2016-03-27 09:50:00'],
    [0, '2017-03-27 03:55:00', '2017-03-28 10:30:00'],
], columns=["Id", "Sleep Start", "Sleep End"])
seq4['Sleep Start'] = pd.to_datetime(seq4['Sleep Start'])
seq4['Sleep End'] = pd.to_datetime(seq4['Sleep End'])

print(operator.process(seq1)[0])
print(operator.process(seq2)[0])
print(operator.process(seq3)[0])
print(operator.process(seq4)[0])


# -


