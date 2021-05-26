"""
Operator to generate report on validation
"""
from tasrif.processing_pipeline import ValidatorOperator
from tasrif.processing_pipeline import InvCode


class ValidationReportOperator(ValidatorOperator):
    """
    Generates a report from the actual state of flagged days.
    Days included in this report will be removed if use runs ''remove_flagged_days``.

    :return: None
    """
    def __init__(self, **kwargs):
        """Creates a new instance of FlagDayIfNotEnoughConsecutiveDaysOperator

        Parameters
        ----------
        min_activity_threshold: Integer threshold. Default: 0
        **kwargs: arguments passed to ValidatorOperator

        """
        super().__init__(**kwargs)

    def process(self, *data_frames):
        """
        Generates a report from the actual state of flagged days.
        Days included in this report will be removed if use runs ''remove_flagged_days``.
        """
        data_frames = super().process(*data_frames)

        day_related_checks = [
            InvCode.FLAG_DAY_NOT_ENOUGH_VALID_EPOCHS,
            InvCode.FLAG_DAY_NOT_ENOUGH_CONSECUTIVE_DAYS,
            InvCode.FLAG_EPOCH_NON_WEARING,
            InvCode.FLAG_EPOCH_NULL_VALUE,
            InvCode.FLAG_DAY_SHORT_COL_SUM,
            InvCode.FLAG_DAY_LONG_COL_SUM,
            InvCode.FLAG_DAY_NOT_ENOUGH_VALID_EPOCHS,
            InvCode.FLAG_DAY_NOT_ENOUGH_CONSECUTIVE_DAYS,
            InvCode.FLAG_DAY_TOO_MANY_INVALID_EPOCHS,
            InvCode.FLAG_PATIENT_NOT_ENOUGH_DAYS,
        ]

        total_days = 0
        for check in day_related_checks:
            n_days_check_failed = 0
            for data_frame in data_frames:
                data_frame["_tmp_flag_"] = self.flag_list_or(
                    data_frame, [check])
                n_days_check_failed += data_frame.groupby(
                    [self.pid_col,
                     self.experiment_day_col])["_tmp_flag_"].all().sum()
            total_days += n_days_check_failed
            print("Number of days removed due to %s: %d" %
                  (check, n_days_check_failed))

        print(
            "Total number of potential days to remove (may have overlaps): %d"
            % total_days)

        return total_days
