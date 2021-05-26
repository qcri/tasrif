"""
Operator to flag
"""
import pandas as pd

from tasrif.processing_pipeline import ValidatorOperator
from tasrif.processing_pipeline import InvCode


class FlagDayIfNotEnoughConsecutiveDaysOperator(ValidatorOperator):
    """
    In case the number of consecutive days is smaller than ``min_number_days``, we mark all as invalid.
    We also try to find any subset that has at least ``min_number_days``.

    :param min_number_days: minimum number of consecutive days
    """
    def __init__(self, min_number_days: int = 0, **kwargs):
        """Creates a new instance of FlagDayIfNotEnoughConsecutiveDaysOperator

        Parameters
        ----------
        min_activity_threshold: Integer threshold. Default: 0
        **kwargs: arguments passed to ValidatorOperator

        """
        super().__init__(**kwargs)
        self.min_number_days = min_number_days

    def process(self, *data_frames):
        """
        In case the number of consecutive days is smaller than ``min_number_days``, we mark all as invalid.
        We also try to find any subset that has at least ``min_number_days``.
        """
        data_frames = super().process(*data_frames)

        processed = []
        for data_frame in data_frames:
            # Mark activity smaller than minimal
            days_per_patient = data_frame.set_index(
                [self.pid_col,
                 self.experiment_day_col]).index.unique().tolist()
            days_per_patient = pd.DataFrame(
                days_per_patient,
                columns=[self.pid_col, self.experiment_day_col])

            days = days_per_patient.groupby(
                self.pid_col)[self.experiment_day_col].count()

            if len(days) == 0:
                continue

            sorted_days = sorted(days)

            consecutive = 1
            last_value = sorted_days[0]
            saved_so_far = [last_value]
            okay = []

            for actual in sorted_days[1:]:
                if actual == last_value + 1:
                    consecutive += 1
                    last_value = actual
                    saved_so_far.append(last_value)

                else:
                    # Ops! We found a gap in the sequence.
                    # First we check if we already have enough days:
                    if len(saved_so_far) >= self.min_number_days:
                        okay.extend(saved_so_far)  # Cool! We have enough days.

                    else:  # Otherwise we start over
                        consecutive = 1
                        last_value = actual
                        saved_so_far = [last_value]

            if len(saved_so_far) >= self.min_number_days:
                okay.extend(saved_so_far)

            # In the okay set, we have all days that we can keep.
            new_invalid = set(days) - set(okay)

            data_frame.loc[
                data_frame[self.experiment_day_col].isin(new_invalid), self.
                invalid_col] |= InvCode.FLAG_DAY_NOT_ENOUGH_CONSECUTIVE_DAYS
            processed.append(data_frame)
        return processed
