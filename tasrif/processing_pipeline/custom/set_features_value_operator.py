"""
Operator to select column feature_names with the option to set values for the selected data frame
"""
from tasrif.processing_pipeline import ProcessingOperator


class SetFeaturesValueOperator(ProcessingOperator):
    """

    Selects a datafram using the lambda function self.selector,
    then optionally sets the values of the selected dataframe
    with self.values. if self.values is set, then the original dataframes
    are returned with the selected part is set to self.values

    Examples
    --------

    >>> import pandas as pd
    >>> import numpy as np
    >>>
    >>> from tasrif.processing_pipeline.custom import SetFeaturesValueOperator
    >>>
    >>> df0 = pd.DataFrame([['Tom', 10], ['Alfred', 15], ['Alfred', 18], ['Juli', 14]], columns=['name', 'score'])
    >>> df1 = pd.DataFrame({"name": ['Alfred', 'juli', 'Tom', 'Ali'],
    ...                    "score": [np.nan, 155, 159, 165],
    ...                    "born": [pd.NaT, pd.Timestamp("2010-04-25"), pd.NaT,
    ...                             pd.NaT]})
    >>>
    >>> print(df0)
    >>> print(df1)
    >>>
    >>> print()
    >>> print('=================================================')
    >>> print('select rows where score >= 13')
    >>> operator = SetFeaturesValueOperator(selector=lambda df: df.score >= 13)
    >>> print(operator.process(df0, df1))
    >>>
    >>> print()
    >>> print('=================================================')
    >>> print('select rows where score >= 13 and set their scores to 15')
    >>> operator = SetFeaturesValueOperator(selector=lambda df: df.score >= 13,
    ...                                     feature_names=['score'],
    ...                                     value=15)
    >>> df0, df1 = operator.process(df0, df1)
    >>> print(df0)
    >>> print(df1)
         name  score
    0     Tom     10
    1  Alfred     15
    2  Alfred     18
    3    Juli     14
         name  score       born
    0  Alfred    NaN        NaT
    1    juli  155.0 2010-04-25
    2     Tom  159.0        NaT
    3     Ali  165.0        NaT
    =================================================
    select rows where age >= 13
    [     name  score
    1  Alfred     15
    2  Alfred     18
    3    Juli     14,    name  score       born
    1  juli  155.0 2010-04-25
    2   Tom  159.0        NaT
    3   Ali  165.0        NaT]
    =================================================
    select rows where score >= 13 and set their scores to 15
         name  score
    0     Tom     10
    1  Alfred     15
    2  Alfred     15
    3    Juli     15
         name  score       born
    0  Alfred    NaN        NaT
    1    juli   15.0 2010-04-25
    2     Tom   15.0        NaT
    3     Ali   15.0        NaT

    """

    def __init__(self, feature_names: list = None, selector: callable = None, value=None):
        """Creates a new instance of CreateFeatureOperator

        Args:
            feature_names (list):
                list of feature_names to select
            selector (callable):
                lambda function that result in pandas row indexing dataframe
                (a dataframe of trues and falses), see example.
            value (int, optional):
                value to replace the selected rows.
        """
        super().__init__()
        self.feature_names = feature_names
        self.selector = selector
        self.value = value
        self.raw_data_frames = None

    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            pd.DataFrame -or- list[pd.DataFrame]
                Processed dataframe(s) resulting from applying the operator
        """

        self.raw_data_frames = data_frames

        processed = []
        for data_frame, raw_data_frame in zip(data_frames, self.raw_data_frames):
            if self.selector and self.feature_names:
                filtered_result = self.selector(data_frame)
                data_frame = data_frame.loc[filtered_result, self.feature_names]
            elif self.selector and (not self.feature_names):
                filtered_result = self.selector(data_frame)
                data_frame = data_frame.loc[filtered_result]
            elif (not self.selector) and self.feature_names:
                data_frame = data_frame[self.feature_names]

            if self.value is not None:
                raw_data_frame.loc[data_frame.index, data_frame.columns] = self.value
                data_frame = raw_data_frame

            processed.append(data_frame)

        return processed
