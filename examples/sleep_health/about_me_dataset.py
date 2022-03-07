"""
AboutMe Dataset details can be found online at``https://www.synapse.org/#!Synapse:syn18492837/wiki/592581``.

Some important stats:
    - This dataset contains unique data for 3448 participants.
    - ``alcohol`` has 10 NAs (10/3448 = 0.29%)
    - ``basic_expenses`` has 20 NAs (20/3448 = 0.58%)
    - ``caffeine`` has 30 NAs (30/3448 = 0.87%)
    - ``daily_activities`` has 6 NAs (6/3448 = 0.17%)
    - ``daily_smoking`` has 11 NAs (11/3448 = 0.32%)
    - ``education`` has 17 NAs (17/3448 = 0.49%)
    - ``flexible_work_hours`` has 105 NAs (105/3448 = 3.05%)
    - ``gender`` has 8 NAs (8/3448 = 0.23%)
    - ``good_life`` has 10 NAs (10/3448 = 0.29%)
    - ``hispanic`` has 7 NAs (7/3448 = 0.20%)
    - ``income`` has 28 NAs (28/3448 = 0.81%)
    - ``marital`` has 7 NAs (7/3448 = 0.20%)
    - ``race`` has 8 NAs (8/3448 = 0.23%)
    - ``smoking_status`` has 21 NAs (21/3448 = 0.61%)
    - ``weight`` has 59 NAs (59/3448 = 1.71%)
    - ``menopause`` has 2340 NAs (2340/3448 = 67.87%)
    - ``recent_births`` has 2309 NAs (2309/3448 = 66.97%)
    - ``current_pregnant`` has 3438 NAs (3438/3448 = 99.71%)
    - ``work_schedule`` has 110 NAs (110/3448 = 3.19%)

The default behavior of this module is to:
 1. remove NAs for participants in all columns, but``menopause``, ``recent_births`` and``current_pregnant``.
 2. Drop duplicates based on participant id, retaining the last occurrence of a participant id.

The default final dataset size is 3019.

"""
import os

from tasrif.data_readers.sleep_health import SleepHealthDataset
from tasrif.processing_pipeline import SequenceOperator
from tasrif.processing_pipeline.pandas import DropDuplicatesOperator, DropNAOperator

sleephealth_path = os.environ["SLEEPHEALTH"]

pipeline = SequenceOperator(
    [
        SleepHealthDataset(sleephealth_path, "aboutme"),
        DropNAOperator(
            subset=[
                "alcohol",
                "basic_expenses",
                "caffeine",
                "daily_activities",
                "daily_smoking",
                "education",
                "flexible_work_hours",
                "gender",
                "good_life",
                "hispanic",
                "income",
                "race",
                "work_schedule",
                "weight",
                "smoking_status",
                "marital",
            ]
        ),
        DropDuplicatesOperator(subset="participantId", keep="last"),
    ]
)

df = pipeline.process()

print(df)
