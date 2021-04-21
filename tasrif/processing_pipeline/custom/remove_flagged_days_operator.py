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
        super().process(*data_frames)

        processed = []
        total_days_removed = 0
        for data_frame in data_frames:
            experiment_days = list(data_frame[self.experiment_day_col].unique())
            all_days = set(experiment_days)
            valid_days = set(self.get_valid_days(data_frame))
            data_frame = data_frame[data_frame[self.experiment_day_col].isin(valid_days)].copy()

            # Get the length of the invalid_days without having to run get_invalid_daysL
            total_days_removed += len(all_days - valid_days)
            processed.append(data_frame)


        return processed
