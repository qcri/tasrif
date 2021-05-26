"""
Operator to flag days if sum of col values is smaller than threshold
"""
import pandas as pd

from tasrif.processing_pipeline import ValidatorOperator
from tasrif.processing_pipeline import InvCode

class FlagPatientIfNotEnoughDaysOperator(ValidatorOperator):
    """
    Marks as invalid (InvCode.FLAG_PATIENT_NOT_ENOUGH_DAYS) if number of days
    for the patient is less than ``days_threshold``.

    :param max_invalid_minutes_per_day: Integer threshold. Default: 0
    :return: None
    """

    def __init__(self,
                 days_threshold: int = 0,
                 **kwargs):
        """Creates a new instance of FlagEpochActivityLargerThanOperator

        Parameters
        ----------
        features : list
            list of features to select
        conditions : lambda function that result in pandas row indexing dataframe
        (a dataframe of trues and falses), see example.
        **kwargs: arguments passed to ValidatorOperator
        """
        super().__init__(**kwargs)
        self.days_threshold = days_threshold


    def process(self, *data_frames):
        """
        Marks as invalid (InvCode.FLAG_DAY_SHORT_COL_SUM) if activity is less than sum_threshold
        """
        data_frames = super().process(*data_frames)

        processed = []
        for data_frame in data_frames:
            days_per_patient = data_frame.set_index([self.pid_col, self.experiment_day_col]).index.unique().tolist()
            days_per_patient = pd.DataFrame(days_per_patient, columns=[self.pid_col, self.experiment_day_col])

            patient_days = days_per_patient.groupby(self.pid_col)[self.experiment_day_col].count()

            patients_without_enough_days = patient_days < self.days_threshold
            patients_without_enough_days = patients_without_enough_days[patients_without_enough_days].index.tolist()

            data_frame = data_frame.set_index(self.pid_col)
            data_frame.loc[patients_without_enough_days, self.invalid_col] |= InvCode.FLAG_PATIENT_NOT_ENOUGH_DAYS
            data_frame = data_frame.reset_index()

            processed.append(data_frame)
        return processed
