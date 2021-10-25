"""
My Health Dataset details can be found online at ``https://www.synapse.org/#!Synapse:syn18492837/wiki/593707``.
It contains a set of questions about many diseases.
For a disease X (X in Allergies, anxiety, apnea, asthma, atrial, hi_blood_pressure, etc.),

the following questions were made:
    - age_X            ('Age when participant was diagnosed with X')
    - current_X        ('Are you currently being treated for X?')
    - impactsleep_X    ('Does X impact your sleep?'')
    - sleepimpact_X    ('Does sleep impact your X?')

Question about the general health of the participant were also asked:
    - (compare_one_year) 'What is your general health rating compared to one year ago?'
    - (day_to_day) 'In past 7 days, how much did pain interfere with your day-to-day activities?'

    ... These questions had categorical answers.

Some important stats:
    - Original shape is (1551, 114)
    - This dataset contains 1551 rows for 1478 unique participants.
    - ``participantId`` has 0 NAs ( 1551 / 1551 ) = 0.00 %
    - ``age_allergies`` has 867 NAs ( 684 / 1551 ) = 55.90 %
    - ``current_allergies`` has 833 NAs ( 718 / 1551 ) = 53.71 %
    - ``impactsleep_allergies`` has 833 NAs ( 718 / 1551 ) = 53.71 %
    - ``sleepimpact_allergies`` has 835 NAs ( 716 / 1551 ) = 53.84 %
    - ``allergies`` has 3 NAs ( 1548 / 1551 ) = 0.19 %
    - ``anxiety`` has 3 NAs ( 1548 / 1551 ) = 0.19 %
    - ``age_anxiety`` has 1094 NAs ( 457 / 1551 ) = 70.54 %
    - ``current_anxiety`` has 1085 NAs ( 466 / 1551 ) = 69.95 %
    - ``impactsleep_anxiety`` has 1085 NAs ( 466 / 1551 ) = 69.95 %
    - ``sleepimpact_anxiety`` has 1084 NAs ( 467 / 1551 ) = 69.89 %
    - ``anxious`` has 1 NAs ( 1550 / 1551 ) = 0.06 %
    - ``apnea`` has 3 NAs ( 1548 / 1551 ) = 0.19 %
    - ``age_apnea`` has 1228 NAs ( 323 / 1551 ) = 79.17 %
    - ``current_apnea`` has 1223 NAs ( 328 / 1551 ) = 78.85 %
    - ``impactsleep_apnea`` has 1223 NAs ( 328 / 1551 ) = 78.85 %
    - ``sleepimpact_apnea`` has 1227 NAs ( 324 / 1551 ) = 79.11 %
    - ``asthma`` has 2 NAs ( 1549 / 1551 ) = 0.13 %
    - ``age_asthma`` has 1242 NAs ( 309 / 1551 ) = 80.08 %
    - ``current_asthma`` has 1239 NAs ( 312 / 1551 ) = 79.88 %
    - ``impactsleep_asthma`` has 1239 NAs ( 312 / 1551 ) = 79.88 %
    - ``sleepimpact_asthma`` has 1239 NAs ( 312 / 1551 ) = 79.88 %
    - ``atrial`` has 0 NAs ( 1551 / 1551 ) = 0.00 %
    - ``age_atrial`` has 1521 NAs ( 30 / 1551 ) = 98.07 %
    - ``current_atrial`` has 1521 NAs ( 30 / 1551 ) = 98.07 %
    - ``impactsleep_atrial`` has 1521 NAs ( 30 / 1551 ) = 98.07 %
    - ``sleepimpact_atrial`` has 1521 NAs ( 30 / 1551 ) = 98.07 %
    - ``hi_blood_pressure`` has 1 NAs ( 1550 / 1551 ) = 0.06 %
    - ``age_hbp`` has 1269 NAs ( 282 / 1551 ) = 81.82 %
    - ``current_hbp`` has 1264 NAs ( 287 / 1551 ) = 81.50 %
    - ``impactsleep_hbp`` has 1264 NAs ( 287 / 1551 ) = 81.50 %
    - ``sleepimpact_hbp`` has 1265 NAs ( 286 / 1551 ) = 81.56 %
    - ``cancer`` has 0 NAs ( 1551 / 1551 ) = 0.00 %
    - ``age_cancer`` has 1486 NAs ( 65 / 1551 ) = 95.81 %
    - ``current_cancer`` has 1486 NAs ( 65 / 1551 ) = 95.81 %
    - ``impactsleep_cancer`` has 1486 NAs ( 65 / 1551 ) = 95.81 %
    - ``sleepimpact_cancer`` has 1486 NAs ( 65 / 1551 ) = 95.81 %
    - ``cardiovascular`` has 7 NAs ( 1544 / 1551 ) = 0.45 %
    - ``compare_one_year`` has 1 NAs ( 1550 / 1551 ) = 0.06 %
    - ``day_to_day`` has 3 NAs ( 1548 / 1551 ) = 0.19 %
    - ``depressed`` has 1 NAs ( 1550 / 1551 ) = 0.06 %
    - ``depression`` has 2 NAs ( 1549 / 1551 ) = 0.13 %
    - ``age_depression`` has 1004 NAs ( 547 / 1551 ) = 64.73 %
    - ``current_depression`` has 997 NAs ( 554 / 1551 ) = 64.28 %
    - ``impactsleep_depression`` has 999 NAs ( 552 / 1551 ) = 64.41 %
    - ``sleepimpact_depression`` has 997 NAs ( 554 / 1551 ) = 64.28 %
    - ``diabetes`` has 0 NAs ( 1551 / 1551 ) = 0.00 %
    - ``age_diabetes`` has 1461 NAs ( 90 / 1551 ) = 94.20 %
    - ``current_Diabetes`` has 1459 NAs ( 92 / 1551 ) = 94.07 %
    - ``impactsleep_diabetes`` has 1459 NAs ( 92 / 1551 ) = 94.07 %
    - ``sleepimpact_diabetes`` has 1460 NAs ( 91 / 1551 ) = 94.13 %
    - ``diabetes_type`` has 1460 NAs ( 91 / 1551 ) = 94.13 %
    - ``emotional`` has 3 NAs ( 1548 / 1551 ) = 0.19 %
    - ``erectile`` has 5 NAs ( 1546 / 1551 ) = 0.32 %
    - ``age_ed`` has 1477 NAs ( 74 / 1551 ) = 95.23 %
    - ``current_ed`` has 1476 NAs ( 75 / 1551 ) = 95.16 %
    - ``impactsleep_ed`` has 1476 NAs ( 75 / 1551 ) = 95.16 %
    - ``sleepimpact_ed`` has 1475 NAs ( 76 / 1551 ) = 95.10 %
    - ``fatigued`` has 3 NAs ( 1548 / 1551 ) = 0.19 %
    - ``gastroesophageal`` has 0 NAs ( 1551 / 1551 ) = 0.00 %
    - ``age_gastroesophageal`` has 1289 NAs ( 262 / 1551 ) = 83.11 %
    - ``current_gastroesophageal`` has 1284 NAs ( 267 / 1551 ) = 82.79 %
    - ``impactsleep_gastroesophageal`` has 1284 NAs ( 267 / 1551 ) = 82.79 %
    - ``sleepimpact_gastroesophageal`` has 1285 NAs ( 266 / 1551 ) = 82.85 %
    - ``general_health`` has 3 NAs ( 1548 / 1551 ) = 0.19 %
    - ``health_care`` has 30 NAs ( 1521 / 1551 ) = 1.93 %
    - ``heart_disease`` has 0 NAs ( 1551 / 1551 ) = 0.00 %
    - ``age_heart_disease`` has 1514 NAs ( 37 / 1551 ) = 97.61 %
    - ``current_heart_disease`` has 1513 NAs ( 38 / 1551 ) = 97.55 %
    - ``impactsleep_heart_disease`` has 1513 NAs ( 38 / 1551 ) = 97.55 %
    - ``sleepimpact_heart_disease`` has 1514 NAs ( 37 / 1551 ) = 97.61 %
    - ``insomnia`` has 1 NAs ( 1550 / 1551 ) = 0.06 %
    - ``age_insomnia`` has 1320 NAs ( 231 / 1551 ) = 85.11 %
    - ``current_insomnia`` has 1313 NAs ( 238 / 1551 ) = 84.66 %
    - ``impactsleep_insomnia`` has 1313 NAs ( 238 / 1551 ) = 84.66 %
    - ``sleepimpact_insomnia`` has 1313 NAs ( 238 / 1551 ) = 84.66 %
    - ``lung`` has 0 NAs ( 1551 / 1551 ) = 0.00 %
    - ``age_lung`` has 1535 NAs ( 16 / 1551 ) = 98.97 %
    - ``current_lung`` has 1535 NAs ( 16 / 1551 ) = 98.97 %
    - ``impactsleep_lung`` has 1535 NAs ( 16 / 1551 ) = 98.97 %
    - ``sleepimpact_lung`` has 1535 NAs ( 16 / 1551 ) = 98.97 %
    - ``mental_health`` has 0 NAs ( 1551 / 1551 ) = 0.00 %
    - ``narcolepsy`` has 2 NAs ( 1549 / 1551 ) = 0.13 %
    - ``age_Narcolepsy`` has 1516 NAs ( 35 / 1551 ) = 97.74 %
    - ``current_Narcolepsy`` has 1516 NAs ( 35 / 1551 ) = 97.74 %
    - ``impactsleep_Narcolepsy`` has 1515 NAs ( 36 / 1551 ) = 97.68 %
    - ``sleepimpact_Narcolepsy`` has 1515 NAs ( 36 / 1551 ) = 97.68 %
    - ``nocturia`` has 5 NAs ( 1546 / 1551 ) = 0.32 %
    - ``age_Nocturia`` has 1515 NAs ( 36 / 1551 ) = 97.68 %
    - ``current_Nocturia`` has 1515 NAs ( 36 / 1551 ) = 97.68 %
    - ``impactsleep_Nocturia`` has 1515 NAs ( 36 / 1551 ) = 97.68 %
    - ``sleepimpact_Nocturia`` has 1515 NAs ( 36 / 1551 ) = 97.68 %
    - ``restless_legs_syndrome`` has 2 NAs ( 1549 / 1551 ) = 0.13 %
    - ``age_rls`` has 1444 NAs ( 107 / 1551 ) = 93.10 %
    - ``current_rls`` has 1442 NAs ( 109 / 1551 ) = 92.97 %
    - ``impactsleep_rls`` has 1442 NAs ( 109 / 1551 ) = 92.97 %
    - ``sleepimpact_rls`` has 1443 NAs ( 108 / 1551 ) = 93.04 %
    - ``stroke`` has 1 NAs ( 1550 / 1551 ) = 0.06 %
    - ``age_Stroke`` has 1530 NAs ( 21 / 1551 ) = 98.65 %
    - ``current_Stroke`` has 1530 NAs ( 21 / 1551 ) = 98.65 %
    - ``impactsleep_stroke`` has 1530 NAs ( 21 / 1551 ) = 98.65 %
    - ``sleepimpact_stroke`` has 1530 NAs ( 21 / 1551 ) = 98.65 %
    - ``physical_activities`` has 2 NAs ( 1549 / 1551 ) = 0.13 %
    - ``physical_health`` has 3 NAs ( 1548 / 1551 ) = 0.19 %
    - ``risk`` has 5 NAs ( 1546 / 1551 ) = 0.32 %
    - ``sleep_trouble`` has 8 NAs ( 1543 / 1551 ) = 0.52 %
    - ``social_activities`` has 2 NAs ( 1549 / 1551 ) = 0.13 %
    - ``stressed`` has 0 NAs ( 1551 / 1551 ) = 0.00 %
    - ``uars`` has 2 NAs ( 1549 / 1551 ) = 0.13 %
    - ``age_uars`` has 1546 NAs ( 5 / 1551 ) = 99.68 %
    - ``current_uars`` has 1545 NAs ( 6 / 1551 ) = 99.61 %
    - ``impactsleep_uars`` has 1545 NAs ( 6 / 1551 ) = 99.61 %
    - ``sleepimpact_uars`` has 1545 NAs ( 6 / 1551 ) = 99.61 %
    - ``timestamp`` has 0 NAs ( 1551 / 1551 ) = 0.00 %

The default pipeline:
    1. converts timestamp to datatime, sort the dataframe by time and removes all duplicates entries for the
       same participant id, retaining only the last one;
    2. transforms into NA some values (i.e. answer for a question being "Don't known") in all columns;
    3. Left the disease specific NA rows, but dropped NAs in the general health questions
    4. One hot encode the general health questions

Final dataset shape after default preprocessing pipeline is (1445, 160)

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
    SleepHealthDataset(sleephealth_path, "myhealth"),
    ConvertToDatetimeOperator(feature_names="timestamp",
                              format="%Y-%m-%dT%H:%M:%S%z",
                              utc=True),
    SortOperator(by=["participantId", "timestamp"]),
    DropDuplicatesOperator(subset="participantId", keep="last"),
    ReplaceOperator(
        to_replace={
            "allergies": {
                3: np.nan
            },
            "anxiety": {
                3: np.nan
            },
            "apnea": {
                3: np.nan
            },
            "asthma": {
                3: np.nan
            },
            "atrial": {
                3: np.nan
            },
            "hi_blood_pressure": {
                3: np.nan
            },
            "cancer": {
                3: np.nan
            },
            "depression": {
                3: np.nan
            },
            "diabetes": {
                3: np.nan
            },
            "erectile": {
                3: np.nan
            },
            "gastroesophageal": {
                3: np.nan
            },
            "heart_disease": {
                3: np.nan
            },
            "insomnia": {
                3: np.nan
            },
            "lung": {
                3: np.nan
            },
            "narcolepsy": {
                3: np.nan
            },
            "nocturia": {
                3: np.nan
            },
            "restless_legs_syndrome": {
                3: np.nan
            },
            "stroke": {
                3: np.nan
            },
            "uars": {
                3: np.nan
            },
        }),
    DropNAOperator(subset=[
        "anxious",
        "cardiovascular",
        "compare_one_year",
        "day_to_day",
        "depressed",
        "emotional",
        "fatigued",
        "general_health",
        "mental_health",
        "physical_activities",
        "physical_health",
        "risk",
        "sleep_trouble",
        "social_activities",
        "stressed",
    ]),
    OneHotEncoderOperator(
        feature_names=[
            "anxious",
            "cardiovascular",
            "compare_one_year",
            "day_to_day",
            "depressed",
            "emotional",
            "fatigued",
            "general_health",
            "mental_health",
            "physical_activities",
            "physical_health",
            "risk",
            "sleep_trouble",
            "social_activities",
            "stressed",
        ],
        drop_first=True,
    ),
])


df = pipeline.process()

print(df)
