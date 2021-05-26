"""
Operator to flag columns that contain null
"""
from tasrif.processing_pipeline import ValidatorOperator
from tasrif.processing_pipeline import InvCode

class FlagEpochNullColsOperator(ValidatorOperator):
    """
    For each on of the columns in ``col_list``, this method marks as invalid (InvCode.FLAG_EPOCH_NULL_VALUE) if the value for an epoch is None/Null.

    :param col_list: List of columns to check for Null/None values.
    """

    def __init__(self, 
                 col_list: list,
                 **kwargs):
        """Creates a new instance of CreateFeatureOperator

        Parameters
        ----------
        features : list
            list of features to select
        conditions : lambda function that result in pandas row indexing dataframe
        (a dataframe of trues and falses), see example.
        """
        super().__init__(**kwargs)
        self.col_list = col_list


    def process(self, *data_frames):
        """
        For each on of the columns in ``col_list``, this method marks as invalid (InvCode.FLAG_EPOCH_NULL_VALUE) if the value for an epoch is None/Null.

        :param col_list: List of columns to check for Null/None values.
        """

        data_frames = super().process(*data_frames)

        processed = []
        for data_frame in data_frames:
            for col in self.col_list:
                if col not in data_frame.keys():
                    raise KeyError("Col %s is not available for the dataframe" % (col))

                data_frame.loc[data_frame[col].isnull(), self.invalid_col] |= InvCode.FLAG_EPOCH_NULL_VALUE
                processed.append(data_frame)

        return processed
