"""
Operator to remove flagged days
"""
from tasrif.processing_pipeline import ValidatorOperator

class RemoveFlaggedDaysOperator(ValidatorOperator):
    """
    Fully removes from the wearable data the days that are flagged with problems.
    Only if all epochs in the day are flagged.
    """

    def __init__(self, **kwargs):
        """Creates a new instance of FlagEpochActivityLargerThanOperator
        Parameters
        ----------
        **kwargs: arguments passed to ValidatorOperator
        """
        super().__init__(**kwargs)

    def process(self, *data_frames):
        """
        Fully removes from the wearable data the days that are flagged with problems.
        Only if all epochs in the day are flagged.
        """
        data_frames = super().process(*data_frames)

        processed = []
        for data_frame in data_frames:

            # Valid days in dataframe
            # grp_days assigns True to days that have invalid_col value of non 0 for all the day,
            # False otherwise.
            grp_days = data_frame.groupby([self.pid_col, self.experiment_day_col])[self.invalid_col].all()

            # valid days are invalid_col that have 0s in a particular day.
            valid_days = grp_days[~grp_days]

            # Return the data_frame with valid days only
            data_frame = data_frame.set_index([self.pid_col, self.experiment_day_col]).loc[valid_days.index]
            data_frame = data_frame.reset_index()

            processed.append(data_frame)


        return processed
