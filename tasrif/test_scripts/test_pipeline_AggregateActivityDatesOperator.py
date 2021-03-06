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

import numpy as np
import pandas as pd

from tasrif.processing_pipeline.custom import AggregateActivityDatesOperator

df = pd.DataFrame(
    [
        [122, 1, "2016-03-13 02:39:00", 1],
        [122, 1, "2016-03-13 02:40:00", 1],
        [122, 1, "2016-03-13 02:41:00", 1],
        [122, 1, "2016-03-13 02:42:00", 1],
        [122, 1, "2016-03-13 02:43:00", 1],
        [122, 1, "2016-03-13 02:44:00", 1],
        [122, 1, "2016-03-13 02:45:00", 2],
        [122, 1, "2016-03-13 02:46:00", 2],
        [122, 1, "2016-03-13 02:47:00", 1],
        [122, 1, "2016-03-13 02:48:00", 1],
        [122, 2, "2016-03-13 06:06:00", 1],
        [122, 2, "2016-03-13 06:07:00", 1],
        [122, 2, "2016-03-13 06:08:00", 1],
        [122, 2, "2016-03-13 06:09:00", 1],
        [122, 2, "2016-03-13 06:10:00", 1],
        [122, 2, "2016-03-13 06:11:00", 1],
        [122, 2, "2016-03-13 06:12:00", 1],
        [122, 2, "2016-03-13 06:13:00", 1],
        [122, 2, "2016-03-13 06:14:00", 1],
        [122, 2, "2016-03-13 06:15:00", 1],
        [144, 1, "2016-03-13 06:36:00", 1],
        [144, 1, "2016-03-13 06:37:00", 1],
        [144, 1, "2016-03-13 06:38:00", 1],
        [144, 1, "2016-03-13 06:39:00", 1],
        [144, 1, "2016-03-13 06:40:00", 1],
        [144, 1, "2016-03-13 06:41:00", 1],
        [144, 1, "2016-03-13 06:42:00", 1],
        [144, 1, "2016-03-13 06:43:00", 1],
        [144, 1, "2016-03-13 06:44:00", 2],
        [144, 1, "2016-03-13 06:45:00", 1],
        [167, 1, "2016-03-14 01:32:00", 2],
        [167, 1, "2016-03-14 01:33:00", 2],
        [167, 1, "2016-03-14 01:34:00", 1],
        [167, 1, "2016-03-14 01:35:00", 1],
        [167, 1, "2016-03-14 01:36:00", 1],
        [167, 1, "2016-03-14 01:37:00", 1],
        [167, 1, "2016-03-14 01:38:00", 1],
        [167, 1, "2016-03-14 01:39:00", 1],
        [167, 1, "2016-03-14 01:40:00", 1],
        [167, 1, "2016-03-14 01:41:00", 1],
        [167, 2, "2016-03-15 02:36:00", 2],
        [167, 2, "2016-03-15 02:37:00", 2],
        [167, 2, "2016-03-15 02:38:00", 2],
        [167, 2, "2016-03-15 02:39:00", 3],
        [167, 2, "2016-03-15 02:40:00", 3],
        [167, 2, "2016-03-15 02:41:00", 3],
        [167, 2, "2016-03-15 02:42:00", 3],
        [167, 2, "2016-03-15 02:43:00", 3],
        [167, 2, "2016-03-15 02:44:00", 2],
        [167, 2, "2016-03-15 02:45:00", 1],
        [167, 3, "2016-03-15 03:03:00", 1],
        [167, 3, "2016-03-15 03:04:00", 1],
        [167, 3, "2016-03-15 03:05:00", 1],
        [167, 3, "2016-03-15 03:06:00", 1],
        [167, 3, "2016-03-15 03:07:00", 1],
        [167, 3, "2016-03-15 03:08:00", 1],
        [167, 3, "2016-03-15 03:09:00", 1],
        [167, 3, "2016-03-15 03:10:00", 1],
        [167, 3, "2016-03-15 03:11:00", 1],
        [167, 3, "2016-03-15 03:12:00", 1],
        [167, 4, "2016-03-15 03:58:00", 1],
        [167, 4, "2016-03-15 03:59:00", 1],
        [167, 4, "2016-03-15 04:00:00", 1],
        [167, 4, "2016-03-15 04:01:00", 1],
        [167, 4, "2016-03-15 04:02:00", 1],
        [167, 4, "2016-03-15 04:03:00", 1],
        [167, 4, "2016-03-15 04:04:00", 1],
        [167, 4, "2016-03-15 04:05:00", 1],
        [167, 4, "2016-03-15 04:06:00", 1],
        [167, 4, "2016-03-15 04:07:00", 1],
    ],
    columns=["Id", "logId", "date", "value"],
)

operator = AggregateActivityDatesOperator(
    date_feature_name="date",
    participant_identifier=["Id", "logId"],
    aggregation_definition={"value": [np.sum, lambda x: x[x == 1].sum()]},
)
operator.process(df)[0]
