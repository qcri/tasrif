"""
Operator to flag days if sum of col values is smaller than threshold
"""
from tasrif.processing_pipeline import ValidatorOperator
from tasrif.processing_pipeline import InvCode

class FlagDayIfDaySumSmallerThanOperator(ValidatorOperator):
    """
    Marks as invalid (InvCode.FLAG_DAY_TOO_MANY_INVALID_EPOCHS) if activity is larger than ``max_invalid_minutes_per_day``.

    :param max_invalid_minutes_per_day: Integer threshold. Default: 0
    :return: None
    """

    def __init__(self,
                 col,
                 sum_threshold: int = 0,
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
        self.col = col
        self.sum_threshold = sum_threshold


    def process(self, *data_frames):
        """
        Marks as invalid (InvCode.FLAG_DAY_SHORT_COL_SUM) if activity is less than sum_threshold
        """
        data_frames = super().process(*data_frames)


        processed = []
        for data_frame in data_frames:
            days_smaller_than_threshold = data_frame.groupby([self.pid_col, self.experiment_day_col])[self.col].transform(
                lambda x: x.sum()) < self.sum_threshold
            data_frame.loc[days_smaller_than_threshold, self.invalid_col] |= InvCode.FLAG_DAY_SHORT_COL_SUM
            processed.append(data_frame)

        return processed
