"""
Module that provides classes to work with the MyHeartCounts dataset
   **Available datasets**:
        - MyHeartCountsDataset
        - DailyCheckSurveyDataset
        - DayOneSurveyDataset
        - PARQSurveyDataset
        - RiskFactorSurvey
        - CardioDietSurveyDataset
        - ActivitySleepSurveyDataset
        - HeartAgeSurveyDataset
        - QualityOfLifeSurveyDataset
        - DemographicsSurveyDataset
"""
from tasrif.processing_pipeline.pandas import ReadCsvOperator

class DailyCheckSurveyDataset(ReadCsvOperator):
    """Class to work with the Daily Survey Table in the MyHeartCounts dataset."""

class DayOneSurveyDataset(ReadCsvOperator):
    """Class to work with the Day One Survey Table in the MyHeartCounts dataset."""

    device_mapping = {
        "iPhone": "1",
        "ActivityBand": "2",
        "Pedometer": "2",
        "SmartWatch": "3",
        "AppleWatch": "3",
        "Other": "Other",
    }

class PARQSurveyDataset(ReadCsvOperator):
    """Class to work with the PARQ Survey Table in the MyHeartCounts dataset."""

class RiskFactorSurveyDataset(ReadCsvOperator):
    """Class to work with the Risk factor Survey Table in the MyHeartCounts dataset."""

class CardioDietSurveyDataset(ReadCsvOperator):
    """Class to work with the Cardio diet survey Table in the MyHeartCounts dataset."""

class ActivitySleepSurveyDataset(ReadCsvOperator):
    """Class to work with the Cardio diet survey Table in the MyHeartCounts dataset.

       Some important stats:
           - This dataset contains unique data for  24966 participants.
            - ` recordId ` has 0 NAs ( 24966 / 24966 ) = 0.00 %
            - ` appVersion ` has 0 NAs ( 24966 / 24966 ) = 0.00 %
            - ` phoneInfo ` has 0 NAs ( 24966 / 24966 ) = 0.00 %
            - ` healthCode ` has 0 NAs ( 24966 / 24966 ) = 0.00 %
            - ` createdOn ` has 0 NAs ( 24966 / 24966 ) = 0.00 %
            - ` atwork ` has 4029 NAs ( 20937 / 24966 ) = 16.14 %
            - ` moderate_act ` has 1077 NAs ( 23889 / 24966 ) = 4.31 %
            - ` phys_activity ` has 105 NAs ( 24861 / 24966 ) = 0.42 %
            - ` sleep_diagnosis1 ` has 73 NAs ( 24893 / 24966 ) = 0.29 %
            - ` sleep_time ` has 118 NAs ( 24848 / 24966 ) = 0.47 %
            - ` sleep_time1 ` has 118 NAs ( 24848 / 24966 ) = 0.47 %
            - ` vigorous_act ` has 1065 NAs ( 23901 / 24966 ) = 4.27 %
            - ` work ` has 121 NAs ( 24845 / 24966 ) = 0.48 %
    """

class HeartAgeSurveyDataset(ReadCsvOperator):
    """Class to work with the Cardio diet survey Table in the MyHeartCounts dataset.

    **Some important stats**:
        - This dataset contains unique data for  10772 participants.
            - ` recordId ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` healthCode ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` createdOn ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` appVersion ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` phoneInfo ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` bloodPressureInstruction ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` bloodPressureInstruction_unit ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` heartAgeDataBloodGlucose ` has 1 NAs ( 10771 / 10772 ) = 0.01 %
            - ` heartAgeDataBloodGlucose_unit ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` heartAgeDataDiabetes ` has 2 NAs ( 10770 / 10772 ) = 0.02 %
            - ` heartAgeDataGender ` has 66 NAs ( 10706 / 10772 ) = 0.61 %
            - ` heartAgeDataEthnicity ` has 1 NAs ( 10771 / 10772 ) = 0.01 %
            - ` heartAgeDataHdl ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` heartAgeDataHdl_unit ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` heartAgeDataHypertension ` has 2 NAs ( 10770 / 10772 ) = 0.02 %
            - ` heartAgeDataLdl ` has 1 NAs ( 10771 / 10772 ) = 0.01 %
            - ` heartAgeDataLdl_unit ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` smokingHistory ` has 39 NAs ( 10733 / 10772 ) = 0.36 %
            - ` heartAgeDataSystolicBloodPressure ` has 1 NAs ( 10771 / 10772 ) = 0.01 %
            - ` heartAgeDataSystolicBloodPressure_unit ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` heartAgeDataTotalCholesterol ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` heartAgeDataTotalCholesterol_unit ` has 0 NAs ( 10772 / 10772 ) = 0.00 %
            - ` heartAgeDataAge ` has 91 NAs ( 10681 / 10772 ) = 0.84 %
    """

class QualityOfLifeSurveyDataset(ReadCsvOperator):
    """Class to work with the Quality of Life survey Table in the MyHeartCounts dataset.

    **Some important stats**:
        - Shape: (22614, 15)
        - This dataset contains unique data for  22614 participants.
         - ` recordId ` has 0 NAs ( 22614 / 22614 ) = 0.00 %
         - ` healthCode ` has 0 NAs ( 22614 / 22614 ) = 0.00 %
         - ` createdOn ` has 0 NAs ( 22614 / 22614 ) = 0.00 %
         - ` appVersion ` has 0 NAs ( 22614 / 22614 ) = 0.00 %
         - ` phoneInfo ` has 0 NAs ( 22614 / 22614 ) = 0.00 %
         - ` feel_worthwhile1 ` has 63 NAs ( 22551 / 22614 ) = 0.28 %
         - ` feel_worthwhile2 ` has 78 NAs ( 22536 / 22614 ) = 0.34 %
         - ` feel_worthwhile3 ` has 89 NAs ( 22525 / 22614 ) = 0.39 %
         - ` feel_worthwhile4 ` has 198 NAs ( 22416 / 22614 ) = 0.88 %
         - ` riskfactors1 ` has 35 NAs ( 22579 / 22614 ) = 0.15 %
         - ` riskfactors2 ` has 55 NAs ( 22559 / 22614 ) = 0.24 %
         - ` riskfactors3 ` has 72 NAs ( 22542 / 22614 ) = 0.32 %
         - ` riskfactors4 ` has 85 NAs ( 22529 / 22614 ) = 0.38 %
         - ` satisfiedwith_life ` has 58 NAs ( 22556 / 22614 ) = 0.26 %
         - ` zip3 ` has 538 NAs ( 22076 / 22614 ) = 2.38 %
    """

class DemographicsSurveyDataset(ReadCsvOperator):
    """Class to work with the Demographics survey Table in the MyHeartCounts dataset.

    **Some important stats**:
        - Shape: (12439, 11)
        - This dataset contains unique data for  12439 participants.
        - ``recordId`` has 0 NAs ( 12439 / 12439 ) = 0.00 %
        - ``appVersion`` has 0 NAs ( 12439 / 12439 ) = 0.00 %
        - ``phoneInfo`` has 0 NAs ( 12439 / 12439 ) = 0.00 %
        - ``healthCode`` has 0 NAs ( 12439 / 12439 ) = 0.00 %
        - ``createdOn`` has 0 NAs ( 12439 / 12439 ) = 0.00 %
        - ``patientWeightPounds`` has 8066 NAs ( 4373 / 12439 ) = 64.84 %
        - ``patientBiologicalSex`` has 7655 NAs ( 4784 / 12439 ) = 61.54 %
        - ``patientHeightInches`` has 7952 NAs ( 4487 / 12439 ) = 63.93 %
        - ``patientWakeUpTime`` has 7666 NAs ( 4773 / 12439 ) = 61.63 %
        - ``patientCurrentAge`` has 9568 NAs ( 2871 / 12439 ) = 76.92 %
        - ``patientGoSleepTime`` has 7615 NAs ( 4824 / 12439 ) = 61.22 %

    """

class HealthKitDataDataset(ReadCsvOperator):
    """Class to work with the Health Kit Data survey Table in the MyHeartCounts dataset.

    **Some important stats**:
        - Shape: (116951, 6)
        - This dataset contains unique data for  116951 participants.
         - ` recordId ` has 0 NAs ( 116951 / 116951 ) = 0.00 %
         - ` appVersion ` has 0 NAs ( 116951 / 116951 ) = 0.00 %
         - ` phoneInfo ` has 0 NAs ( 116951 / 116951 ) = 0.00 %
         - ` healthCode ` has 0 NAs ( 116951 / 116951 ) = 0.00 %
         - ` createdOn ` has 0 NAs ( 116951 / 116951 ) = 0.00 %
         - ` data.csv ` has 0 NAs ( 116951 / 116951 ) = 0.00 %
    """

class SixMinuteWalkActivityDataset(ReadCsvOperator):
    """
    Class to work with the Six Minute Walk Activity table
    """
