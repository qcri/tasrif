"""Module that defines the NoopOperator class
"""


from enum import IntFlag
from tasrif.processing_pipeline import ProcessingOperator
import pandas as pd

class InvCode(IntFlag):
    """
    Base class for creating enumerated constants
    that can be combined using the bitwise operators without
    losing their IntFlag membership.
    IntFlag members are also subclasses of int.
    """
    FLAG_OKAY = 0
    FLAG_EPOCH_PA = 1
    FLAG_EPOCH_NON_WEARING = 2
    FLAG_EPOCH_NULL_VALUE = 4
    FLAG_DAY_SHORT_COL_SUM = 8
    FLAG_DAY_LONG_COL_SUM = 16
    FLAG_DAY_NOT_ENOUGH_VALID_EPOCHS = 32
    FLAG_DAY_NOT_ENOUGH_CONSECUTIVE_DAYS = 64
    FLAG_DAY_TOO_MANY_INVALID_EPOCHS = 128
    FLAG_PATIENT_NOT_ENOUGH_DAYS = 256

    @staticmethod
    def check_flag(int_value):
        """Returns true if int_value is 0, false otherwise.

        """
        return InvCode.FLAG_OKAY | int_value


class ValidatorOperator(ProcessingOperator):
    """Class representing a validator operator.
    """

    def __init__(self,
                 time_col='time',
                 pid_col='patientID',
                 experiment_day_col='exp_day',
                 hour_start_experiment=0):
        """Constructs a validator operator

        Examples
        --------


        """
        self.invalid_col = "invalid_code"
        self.time_col = time_col
        self.pid_col = pid_col
        self.experiment_day_col = experiment_day_col
        self.hour_start_experiment = hour_start_experiment

    def process(self, *args):
        """This function returned the received input without any changes (unmutated).
        """

        # Create invalid code column if it doesn't exist
        data = args
        processed = []
        for data_frame in data:
            data_frame = data_frame.copy()
            if self.invalid_col not in data_frame.columns:
                data_frame[self.invalid_col] = InvCode.FLAG_OKAY

            # Create day column if it doesn't exist
            if self.experiment_day_col not in data_frame.columns:
                exp_days = data_frame.groupby(self.pid_col).apply(self._add_exp_day).values
                data_frame[self.experiment_day_col] = exp_days

            processed.append(data_frame)

        return tuple(processed)

    def _add_exp_day(self, data_frame):
        """
        Internal method to create exp_day col per id_col
        """
        day_zero = data_frame.iloc[0][self.time_col].toordinal()
        new_exp_day = (data_frame[self.time_col] - pd.DateOffset(hours=0)).apply(lambda x, day_zero=day_zero: x.toordinal() - day_zero)
        return new_exp_day

    def flag_list_or(self, data_frame, list_of_flags: list):
        """
        Internal method to evaluate if an InvCode was used or not.

        :param wearable: Wearable device
        :param list_of_flags: list of InvCode flags.
        :return: A pandas Series object with True if any of the InvCode were used in a given epoch.
        """

        result = data_frame[self.invalid_col].apply(lambda x: list_of_flags[0] in InvCode.check_flag(x))
        for flag in list_of_flags[1:]:
            result |= data_frame[self.invalid_col].apply(lambda x, flag=flag: flag in InvCode.check_flag(x))
        return result
