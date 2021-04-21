"""
Operator to flag days if valid epochs are larger than some value
"""
from tasrif.processing_pipeline import ValidatorOperator
from tasrif.processing_pipeline import InvCode

class FlagDayIfValidEpochsLargerThanOperator(ValidatorOperator):
    """
    Marks as invalid (InvCode.FLAG_DAY_TOO_MANY_INVALID_EPOCHS) if activity is larger than ``max_invalid_minutes_per_day``.

    :param max_invalid_minutes_per_day: Integer threshold. Default: 0
    :return: None
    """

    def __init__(self, 
                 max_invalid_minutes_per_day: int = 0,
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
        self.max_invalid_minutes_per_day = max_invalid_minutes_per_day


    def process(self, *data_frames):
        """
        Marks as invalid (InvCode.FLAG_DAY_TOO_MANY_INVALID_EPOCHS) if activity is above ``max_invalid_epochs_per_day``.

        :param max_invalid_minutes_per_day: Integer threshold. Default: 0
        """
        super().process(*data_frames)

        processed = []
        for data_frame in data_frames:
            max_invalid_epochs_per_day = self.max_invalid_minutes_per_day

            data_frame["_tmp_flag_"] = data_frame[self.invalid_col] == InvCode.FLAG_OKAY

            invalid_epochs = data_frame.groupby([self.experiment_day_col])["_tmp_flag_"].transform(
                lambda x: x.sum()) > max_invalid_epochs_per_day
            data_frame.loc[invalid_epochs, self.invalid_col] |= InvCode.FLAG_DAY_TOO_MANY_INVALID_EPOCHS
            del data_frame["_tmp_flag_"]
            processed.append(data_frame)
        return processed
