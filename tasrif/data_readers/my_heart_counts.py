"""Module that provides classes to work with the MyHeartCounts dataset
   Available datasets:
        MyHeartCountsDataset
        DailyCheckSurveyDataset
        DayOneSurveyDataset
        PARQSurveyDataset
        RiskFactorSurvey
        CardioDietSurveyDataset
        ActivitySleepSurveyDataset
        HeartAgeSurveyDataset
        QualityOfLifeSurveyDataset
        DemographicsSurveyDataset
"""
import os
import pandas as pd

from tasrif.processing_pipeline import ProcessingPipeline
from tasrif.processing_pipeline.custom import SetFeaturesValueOperator
from tasrif.processing_pipeline.custom import CreateFeatureOperator
from tasrif.processing_pipeline.custom import AggregateOperator
from tasrif.processing_pipeline.custom import IterateCsvOperator
from tasrif.processing_pipeline.custom import IterateJsonOperator
from tasrif.processing_pipeline.pandas import DropFeaturesOperator
from tasrif.processing_pipeline.pandas import DropDuplicatesOperator
from tasrif.processing_pipeline.pandas import DropNAOperator
from tasrif.processing_pipeline.pandas import ConvertToDatetimeOperator
from tasrif.processing_pipeline.pandas import SetIndexOperator
from tasrif.processing_pipeline.pandas import PivotResetColumnsOperator
from tasrif.processing_pipeline.pandas import JsonNormalizeOperator

class MyHeartCountsDataset:  # pylint: disable=too-few-public-methods
    """
    Class to work with Standford meds public dataset MyHeartCounts
    """
    def __init__(self, mhc_folder):
        self.mhc_folder = mhc_folder


class DailyCheckSurveyDataset:
    """Class to work with the Daily Survey Table in the MyHeartCounts dataset."""
    class Defaults:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.


        Default PIPELINE Pseudocode
        Modifies self.dcs_df by dropping unnecessary columns (features),
        filling null activity intensity,
        filling null activity time values,
        and averages activity1 and activity2 if given the option.


        Pseudocode:
            - start with raw dataframe dcs_df
            - drop self.drop_features features from dcs_df
            - set activity1_option null values to false if activity2_option is filled
            - set activity1_intensity to 4 if activity1_option and activity1_time are filled
            - set activity1_intensity and activity1_time to 0 if activity1_option is false
            - repeat the three steps above for activity2_option
            - drop activity1_option and activity2_option from dcs_df
            - average the intensities if self.merge_activity_features is true, set it to column 'activity_intensity'
            - average the time if self.merge_activity_features is true, set it to column 'activity_time'
            - return dcs_df

        """

        PIPELINE = ProcessingPipeline([
            DropFeaturesOperator([
                "appVersion",
                "phoneInfo",
                "activity1_type",
                "activity2_type",
                "phone_on_user",
            ]),
            CreateFeatureOperator(
                feature_name="activity1_option",
                feature_creator=lambda df: bool(df["activity1_option"]),
            ),
            CreateFeatureOperator(
                feature_name="activity2_option",
                feature_creator=lambda df: bool(df["activity2_option"]),
            ),
            SetFeaturesValueOperator(
                selector=lambda df: df["activity1_option"].isnull()
                & df["activity2_option"].notnull(),
                features=["activity1_option"],
                value=False,
            ),
            SetFeaturesValueOperator(
                selector=lambda df: df["activity2_option"].isnull()
                & df["activity1_option"].notnull(),
                features=["activity2_option"],
                value=False,
            ),
            SetFeaturesValueOperator(
                selector=lambda df: pd.notnull(df["activity1_option"])
                & df["activity1_option"]
                & pd.notnull(df["activity1_time"])
                & pd.isnull(df["activity1_intensity"]),
                features=["activity1_intensity"],
                value=4,
            ),
            SetFeaturesValueOperator(
                selector=lambda df: pd.notnull(df["activity2_option"])
                & df["activity2_option"]
                & pd.notnull(df["activity2_time"])
                & pd.isnull(df["activity2_intensity"]),
                features=["activity2_intensity"],
                value=4,
            ),
            SetFeaturesValueOperator(
                selector=lambda df: pd.notnull(df["activity1_option"])
                & ~df["activity1_option"],
                features=["activity1_intensity", "activity1_time"],
                value=0,
            ),
            SetFeaturesValueOperator(
                selector=lambda df: pd.notnull(df["activity2_option"])
                & ~df["activity2_option"],
                features=["activity2_intensity", "activity2_time"],
                value=0,
            ),
            DropFeaturesOperator(["activity1_option", "activity2_option"]),
            CreateFeatureOperator(
                feature_name="activity_intensity",
                feature_creator=lambda df:
                (df["activity1_intensity"] + df["activity2_intensity"]) / 2,
            ),
            CreateFeatureOperator(
                feature_name="activity_time",
                feature_creator=lambda df:
                (df["activity1_time"] + df["activity2_time"]) / 2,
            ),
        ])
        """ Default GROUP_PIPELINE Pseudocode
        Modifies self.dcs_df by dropping unnecessary columns (features),
        filling null activity intensity,
        filling null activity time values,
        and averages activity1 and activity2 if given the option.


        Pseudocode:
            - start with self.dcs_df
            - for each paricipant (represented by healthCode column) in self.dcs_df
                - get the mean of activity1_intensity, activity1_time
                - get the standard deviation of activity1_intensity, activity1_time
                - repeat for activity2_option
            - if self.merge_activity_features, then do the above with activity_intensity, and activity_time instead
            - set the result in self.group_dcs_df
        """
        GROUP_PIPELINE = ProcessingPipeline([
            DropFeaturesOperator(["recordId"]),
            AggregateOperator(
                groupby_feature_names="healthCode",
                aggregation_definition={
                    "activity1_intensity": ["mean", "std"],
                    "activity1_time": ["mean", "std"],
                    "activity2_intensity": ["mean", "std"],
                    "activity2_time": ["mean", "std"],
                    "sleep_time": ["mean", "std"],
                },
            ),
        ])

    def __init__(
            self,
            dcs_file_path,
            processing_pipeline: ProcessingPipeline = Defaults.PIPELINE,
            group_pipeline: ProcessingPipeline = Defaults.GROUP_PIPELINE,
        ):  # pylint: disable=too-many-arguments

        self.dcs_df = pd.read_csv(dcs_file_path)
        self.raw_df = self.dcs_df.copy()
        self.processing_pipeline = processing_pipeline
        self.group_pipeline = group_pipeline

        self._process()
        self._group()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df["healthCode"].nunique()
        return number_participants

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.dcs_df

    def grouped_dataframe(self):
        """Gets the dataframe grouped by participants. The result is a data frames where each row
        represents exactly one partipant

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """
        return self.group_dcs_df

    def _process(self):
        """
        Returns
        -------
        sets the result in self.dcs_df
        """

        self.dcs_df = self.processing_pipeline.process(self.dcs_df)[0]

    def _group(self):
        """
        Returns
        -------
        sets the result in self.group_dcs_df
        """
        self.group_dcs_df = self.group_pipeline.process(self.dcs_df)[0]


class DayOneSurveyDataset:  # pylint: disable=too-many-arguments
    """Class to work with the Day One Survey Table in the MyHeartCounts dataset."""

    dos_df = None
    raw_df = None

    device_mapping = {
        "iPhone": "1",
        "ActivityBand": "2",
        "Pedometer": "2",
        "SmartWatch": "3",
        "AppleWatch": "3",
        "Other": "Other",
    }

    def __init__(
            self,
            dos_file_path,
            processing_pipeline: ProcessingPipeline = ProcessingPipeline(
                [DropNAOperator(subset=["device", "labwork"])]),
        ):

        self.dos_df = pd.read_csv(dos_file_path)
        self.raw_df = self.dos_df.copy()
        self.processing_pipeline = processing_pipeline

        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df["healthCode"].nunique()
        return number_participants

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.dos_df

    def _process(self):
        """Modifies self.dos_df by dropping na columns given bools,
        self.drop_device_na and self.drop_labwork_na

        Returns
        -------
        sets the result in self.dos_df
        """

        self.dos_df = self.processing_pipeline.process(self.dos_df)[0]


class PARQSurveyDataset:  # pylint: disable=too-many-arguments
    """Class to work with the PARQ Survey Table in the MyHeartCounts dataset."""

    parq_df = None
    raw_df = None

    def __init__(
            self,
            parq_file_path,
            processing_pipeline: ProcessingPipeline = ProcessingPipeline([
                DropNAOperator(subset=[
                    "chestPain",
                    "chestPainInLastMonth",
                    "dizziness",
                    "heartCondition",
                    "jointProblem",
                    "physicallyCapable",
                    "prescriptionDrugs",
                ]),
                DropDuplicatesOperator(subset=["healthCode"], keep="last"),
            ]),
        ):

        self.parq_df = pd.read_csv(parq_file_path)
        self.raw_df = self.parq_df.copy()
        self.processing_pipeline = processing_pipeline

        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df["healthCode"].nunique()
        return number_participants

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.parq_df

    def _process(self):
        """Modifies self.parq_df by dropping any row that contains
        a null value. drops duplicate rows if self.drop_duplicates
        is true

        Returns
        -------
        sets the result in self.parq_df
        """
        self.parq_df = self.processing_pipeline.process(self.parq_df)[0]


class RiskFactorSurvey:
    """Class to work with the Risk factor Survey Table in the MyHeartCounts dataset."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class."""

        DROP_FEATURES = []

    def __init__(
            self,
            rfs_file_path,
            processing_pipeline: ProcessingPipeline = ProcessingPipeline(
                [DropFeaturesOperator(Default.DROP_FEATURES)]),
        ):

        self.rf_df = pd.read_csv(rfs_file_path)
        self.raw_df = self.rf_df.copy()
        self.processing_pipeline = processing_pipeline

        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df["healthCode"].nunique()
        return number_participants

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.rf_df

    def _process(self):
        self.rf_df = self.processing_pipeline.process(self.rf_df)[0]


class CardioDietSurveyDataset:
    """Class to work with the Cardio diet survey Table in the MyHeartCounts dataset."""
    class Default:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class."""

        DROP_FEATURES = []

    def __init__(
            self,
            cds_file_path,
            processing_pipeline: ProcessingPipeline = ProcessingPipeline(
                [DropFeaturesOperator(Default.DROP_FEATURES)]),
        ):

        self.cd_df = pd.read_csv(cds_file_path)
        self.raw_df = self.cd_df.copy()
        self.processing_pipeline = processing_pipeline

        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df["healthCode"].nunique()
        return number_participants

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.cd_df

    def _process(self):
        """Modifies self.cd_df by dropping columns (features) that
        are given in self.drop_features

        Raises
        -------
        ValueError if self.drop_features contain a non existent column
        within self.cd_df

        Returns
        -------
        sets the result in self.cd_df
        """

        self.cd_df = self.processing_pipeline.process(self.cd_df)[0]


class ActivitySleepSurveyDataset:
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

           The default behavior of this module is to
            (1) remove NAs for participants in all columns.
            (2) Drop duplicates based on participant id, retaining the last occurrence of a participant id.
    """
    def __init__(
            self,
            aas_file_path,
            processing_pipeline: ProcessingPipeline = ProcessingPipeline([
                DropNAOperator(),
                DropDuplicatesOperator(subset=["healthCode"], keep="last"),
            ]),
        ):

        self.processed_df = pd.read_csv(aas_file_path)
        self.raw_df = self.processed_df.copy()
        self.processing_pipeline = processing_pipeline
        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.processed_df["healthCode"].nunique()
        return number_participants

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.processed_df

    def _process(self):
        """Modifies self.cd_df by dropping columns (features) that
        are given in self.drop_features

        Raises
        -------
        ValueError if self.drop_features contain a non existent column
        within self.cd_df

        Returns
        -------
        sets the result in self.cd_df
        """
        self.processed_df = self.processing_pipeline.process(
            self.processed_df)[0]


class HeartAgeSurveyDataset:
    """Class to work with the Cardio diet survey Table in the MyHeartCounts dataset.

    Some important stats:
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

            The default behavior of this module is to
             (1) remove NAs for participants in all columns.
             (2) Drop duplicates based on participant id, retaining the last occurrence of a participant id.
             The default final dataset size is 3019.
    """
    def __init__(
            self,
            has_file_path,
            processing_pipeline: ProcessingPipeline = ProcessingPipeline([
                DropNAOperator(),
                DropDuplicatesOperator(subset=["healthCode"], keep="last"),
            ]),
        ):

        self.processed_df = pd.read_csv(has_file_path)
        self.raw_df = self.processed_df.copy()
        self.processing_pipeline = processing_pipeline
        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.processed_df["healthCode"].nunique()
        return number_participants

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.processed_df

    def _process(self):
        """Modifies self.cd_df by dropping columns (features) that
        are given in self.drop_features

        Raises
        -------
        ValueError if self.drop_features contain a non existent column
        within self.cd_df

        Returns
        -------
        sets the result in self.cd_df
        """
        self.processed_df = self.processing_pipeline.process(
            self.processed_df)[0]


class QualityOfLifeSurveyDataset:
    """Class to work with the Quality of Life survey Table in the MyHeartCounts dataset.

    Some important stats:
    Shape: (22614, 15)
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

        The default behavior of this module is to
         (1) remove NAs for participants in all columns.
         (2) Drop duplicates based on participant id, retaining the last occurrence of a participant id.
         The default final dataset size is 13673.
    """
    def __init__(
            self,
            qol_file_path,
            processing_pipeline: ProcessingPipeline = ProcessingPipeline([
                DropNAOperator(),
                DropDuplicatesOperator(subset=["healthCode"], keep="last"),
            ]),
        ):

        self.processed_df = pd.read_csv(qol_file_path)
        self.raw_df = self.processed_df.copy()
        self.processing_pipeline = processing_pipeline
        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.processed_df["healthCode"].nunique()
        return number_participants

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.processed_df

    def _process(self):
        """Modifies self.processed_df by dropping columns (features) that
        are given in self.drop_features

        Returns
        -------
        sets the result in self.processed_df
        """
        self.processed_df = self.processing_pipeline.process(
            self.processed_df)[0]


class DemographicsSurveyDataset:
    """Class to work with the Demographics survey Table in the MyHeartCounts dataset.

    Some important stats:
    Shape: (12439, 11)
        - This dataset contains unique data for  12439 participants.
         - ` recordId ` has 0 NAs ( 12439 / 12439 ) = 0.00 %
         - ` appVersion ` has 0 NAs ( 12439 / 12439 ) = 0.00 %
         - ` phoneInfo ` has 0 NAs ( 12439 / 12439 ) = 0.00 %
         - ` healthCode ` has 0 NAs ( 12439 / 12439 ) = 0.00 %
         - ` createdOn ` has 0 NAs ( 12439 / 12439 ) = 0.00 %
         - ` patientWeightPounds ` has 8066 NAs ( 4373 / 12439 ) = 64.84 %
         - ` patientBiologicalSex ` has 7655 NAs ( 4784 / 12439 ) = 61.54 %
         - ` patientHeightInches ` has 7952 NAs ( 4487 / 12439 ) = 63.93 %
         - ` patientWakeUpTime ` has 7666 NAs ( 4773 / 12439 ) = 61.63 %
         - ` patientCurrentAge ` has 9568 NAs ( 2871 / 12439 ) = 76.92 %
         - ` patientGoSleepTime ` has 7615 NAs ( 4824 / 12439 ) = 61.22 %

    """
    def __init__(
            self,
            dmo_file_path,
            processing_pipeline: ProcessingPipeline = ProcessingPipeline([
                DropNAOperator(),
                DropDuplicatesOperator(subset=["healthCode"], keep="last"),
            ]),
        ):

        self.processed_df = pd.read_csv(dmo_file_path)
        self.raw_df = self.processed_df.copy()
        self.processing_pipeline = processing_pipeline
        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.processed_df["healthCode"].nunique()
        return number_participants

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.processed_df

    def _process(self):
        """Modifies self.cd_df by dropping columns (features) that
        are given in self.drop_features

        Returns
        -------
        sets the result in self.processed_df
        """
        self.processed_df = self.processing_pipeline.process(
            self.processed_df)[0]


class HealthKitDataDataset:
    """Class to work with the Health Kit Data survey Table in the MyHeartCounts dataset.

    Some important stats:
    Shape: (116951, 6)
        - This dataset contains unique data for  116951 participants.
         - ` recordId ` has 0 NAs ( 116951 / 116951 ) = 0.00 %
         - ` appVersion ` has 0 NAs ( 116951 / 116951 ) = 0.00 %
         - ` phoneInfo ` has 0 NAs ( 116951 / 116951 ) = 0.00 %
         - ` healthCode ` has 0 NAs ( 116951 / 116951 ) = 0.00 %
         - ` createdOn ` has 0 NAs ( 116951 / 116951 ) = 0.00 %
         - ` data.csv ` has 0 NAs ( 116951 / 116951 ) = 0.00 %

        The default behavior of this module is to
         (1) self._process to returns a generator
         (2) the generator iterates through the files found in 'recordId' field
         (3) the generator returns a pandas dataframe per next() call
    """
    class Defaults:  # pylint: disable=too-few-public-methods
        """Default parameters used by the class.
        """
        CSV_PIPELINE = ProcessingPipeline([
            DropNAOperator(),
            ConvertToDatetimeOperator(
                feature_names=["endTime"],
                errors='coerce', utc=True),
            CreateFeatureOperator(
                feature_name='Date',
                feature_creator=lambda df: df['endTime'].date()),
            DropFeaturesOperator(drop_features=['startTime', 'endTime']),
            AggregateOperator(
                groupby_feature_names=["Date", "type"],
                aggregation_definition={'value': 'sum'}),
            SetIndexOperator('Date'),
            PivotResetColumnsOperator(level=1, columns='type')
        ])

        CSV_FOLDER_PATH = os.environ.get('MYHEARTCOUNTS_HEALTHKITDATA_CSV_FOLDER_PATH')

        PIPELINE = ProcessingPipeline([
            CreateFeatureOperator(
                feature_name='file_name',
                feature_creator=lambda df: str(df['data.csv'])),
            IterateCsvOperator(
                folder_path=CSV_FOLDER_PATH,
                field='file_name',
                pipeline=CSV_PIPELINE),
        ])

    def __init__(
            self,
            hkd_file_path,
            processing_pipeline = Defaults.PIPELINE
        ):
        """
        Constructs a new data reader for reading HealthKit data

        Parameters
        ----------
        hkd_file_path: str
            Path to the HealthKit data csv file

        processing_pipeline: ProcessingPipeline
            ProcessingPipeline used to process HealthKit data
        """
        self.processed_df = pd.read_csv(hkd_file_path)
        self.raw_df = self.processed_df.copy()
        self.processing_pipeline = processing_pipeline
        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df["healthCode"].nunique()
        return number_participants

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.processed_df

    def _process(self):
        """Modifies self.processed_df by dropping columns (features) that
        are given in self.drop_features

        Returns
        -------
        sets the result in self.processed_df
        """
        if self.processing_pipeline:
            self.processed_df = self.processing_pipeline.process(
                self.processed_df)

class SixMinuteWalkActivityDataset:
    """
    Class to work with the Six Minute Walk Activity table
    """
    class Defaults: # pylint: disable=too-few-public-methods
        """
        Default parameters used by the class.
        """
        # JSON_PIPELINE is meant to work with pedometer json data from the Six Minute Walk Activity table
        JSON_PIPELINE = ProcessingPipeline([
            JsonNormalizeOperator()
        ])

        JSON_FOLDER_PATH = os.environ.get('MYHEARTCOUNTS_SIXMINUTEWALKACTIVITY_JSON_FOLDER_PATH')

        PIPELINE = ProcessingPipeline([
            CreateFeatureOperator(
                feature_name='file_name',
                # The json filename has an extra '.0' appended to it.
                feature_creator=lambda df: str(df['pedometer_fitness.walk.items'])[:-2]),
            IterateJsonOperator(
                folder_path=JSON_FOLDER_PATH,
                field='file_name',
                pipeline=JSON_PIPELINE),
        ])

    def __init__(
            self,
            smwa_file_path,
            processing_pipeline = Defaults.PIPELINE
        ):
        """
        Constructs a new data reader for reading SixMinuteWalkActivity data

        Parameters
        ----------
        smwa_file_path: str
            Path to SixMinuteWalkActivity data csv file

        processing_pipeline: ProcessingPipeline
            ProcessingPipeline used to process SixMinuteWalkActivity data
        """
        self.processed_df = pd.read_csv(smwa_file_path)
        self.raw_df = self.processed_df.copy()
        self.processing_pipeline = processing_pipeline
        self._process()

    def participant_count(self):
        """Get the number of participants

        Returns
        -------
        int
            Number of participants in the dataset
        """
        number_participants = self.raw_df["healthCode"].nunique()
        return number_participants

    def raw_dataframe(self):
        """Gets the data frame (without any processing) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.raw_df

    def processed_dataframe(self):
        """Gets the processed data frame (after applying the data pipeline) for the dataset

        Returns
        -------
        pd.Dataframe
            Pandas dataframe object representing the data
        """

        return self.processed_df

    def _process(self):
        """Modifies self.processed_df by dropping columns (features) that
        are given in self.drop_features

        Returns
        -------
        sets the result in self.processed_df
        """
        if self.processing_pipeline:
            self.processed_df = self.processing_pipeline.process(
                self.processed_df)
