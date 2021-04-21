"""
Operator to flag dataframes that have activity less than some number
"""
from tasrif.processing_pipeline import ValidatorOperator
from tasrif.processing_pipeline import InvCode

class FlagEpochActivityLessThanOperator(ValidatorOperator):
    """
    Marks as invalid (InvCode.FLAG_PA) if activity is below ``min_activity_threshold``.

    :param min_activity_threshold: Integer threshold. Default: 0
    """

    def __init__(self, 
                 activity_col,
                 min_activity_threshold: int = 0,
                 **kwargs
                 ):
        """Creates a new instance of FlagEpochActivityLessThanOperator

        Parameters
        ----------
        min_activity_threshold: Integer threshold. Default: 0
        **kwargs: arguments passed to ValidatorOperator

        """
        super().__init__(**kwargs)
        self.min_activity_threshold = min_activity_threshold
        self.activity_col = activity_col

    def process(self, *data_frames):
        """
        Marks as invalid (InvCode.FLAG_PA) if physical activity is below ``min_activity_threshold``.

        :param min_activity_threshold: Integer threshold. Default: 0
        """
        super().process(*data_frames)

        processed = []
        for data_frame in data_frames:
            # Mark activity smaller than minimal
            data_frame.loc[data_frame[self.activity_col] < self.min_activity_threshold, self.invalid_col] |= InvCode.FLAG_EPOCH_PA
            processed.append(data_frame)
        return processed
