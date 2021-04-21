"""
Operator to flag days if valid epochs are smaller than some value
"""
from tasrif.processing_pipeline import ValidatorOperator
from tasrif.processing_pipeline import InvCode

class FlagDayIfValidEpochsSmallerThanOperator(ValidatorOperator):
    """
    Marks as invalid the whole day (InvCode.FLAG_DAY_NOT_ENOUGH_VALID_EPOCHS) if the number of valid minutes in a day is smaller than ``valid_minutes_per_day``.

    :param valid_minutes_per_day: Minimum minutes of valid (i.e., without any other flag) in a day.

    """

    def __init__(self, 
                 valid_minutes_per_day: int = 60,
                 **kwargs):
        """Creates a new instance of FlagDayIfValidEpochsSmallerThanOperator

        Parameters
        ----------
        valid_minutes_per_day: Minimum minutes of valid (i.e., without any other flag) in a day.
        **kwargs: arguments passed to ValidatorOperator
        """
        super().__init__(**kwargs)
        self.valid_minutes_per_day = valid_minutes_per_day


    def process(self, *data_frames):
        """
        Marks as invalid the whole day (InvCode.FLAG_DAY_NOT_ENOUGH_VALID_EPOCHS) if the number of valid minutes in a day is smaller than ``valid_minutes_per_day``.

        :param valid_minutes_per_day: Minimum minutes of valid (i.e., without any other flag) in a day.

        """
        super().process(*data_frames)

        processed = []
        for data_frame in data_frames:
            valid_epochs_per_day = self.valid_minutes_per_day

            data_frame["_tmp_flag_"] = data_frame[self.invalid_col] == InvCode.FLAG_OKAY

            invalid_epochs = data_frame.groupby([self.experiment_day_col])["_tmp_flag_"].transform(
                lambda x: x.sum()) <= valid_epochs_per_day
            data_frame.loc[invalid_epochs, self.invalid_col] |= InvCode.FLAG_DAY_NOT_ENOUGH_VALID_EPOCHS
            del data_frame["_tmp_flag_"]
            processed.append(data_frame)
        return processed
