"""
Operator to select column features with the option to set values for the selected data frame
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
    >>> from tasrif.processing_pipeline.custom import SetFeaturesValueOperator
    >>>
    >>> df0 = pd.DataFrame([['tom', 10], ['nick', 15], ['juli', 14]])
    >>> df0.columns = ['name', 'age']
    >>> df1 = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
    ...                    "toy": [np.nan, 'Batmobile', 'Bullwhip'],
    ...                    "age": [11, 14, 17]})
    >>>
    >>> print(df0)
    >>> print(df1)
    .  name  age
    0   tom   10
    1  nick   15
    2  juli   14
    .      name        toy  age
    0    Alfred        NaN   11
    1    Batman  Batmobile   14
    2  Catwoman   Bullwhip   17

    >>> print()
    >>> print('=================================================')
    >>> print('select rows where age >= 13')
    >>> operator = SetFeaturesValueOperator(selector=lambda df: df.age >= 13)
    >>> print(operator.process(df0, df1))
    =================================================
    select rows where age >= 13
    [   name  age
    1  nick   15
    2  juli   14,        name        toy  age
    1    Batman  Batmobile   14
    2  Catwoman   Bullwhip   17]

    >>> print()
    >>> print('=================================================')
    >>> print('select rows where age >= 13 and set their ages to 15')
    >>> operator = SetFeaturesValueOperator(selector=lambda df: df.age >= 13,
    ...                                     features=['age'],
    ...                                     value=15)
    >>> df0, df1 = operator.process(df0, df1)
    >>> print(df0)
    >>> print(df1)
    =================================================
    select rows where age >= 13 and set their ages to 15
    .  name  age
    0   tom   10
    1  nick   15
    2  juli   15
    .      name        toy  age
    0    Alfred        NaN   11
    1    Batman  Batmobile   15
    2  Catwoman   Bullwhip   15

    """

    def __init__(self, features: list = None, selector: callable = None, value=None):
        """Creates a new instance of CreateFeatureOperator

        Args:
            features (list):
                list of features to select
            selector (callable):
                lambda function that result in pandas row indexing dataframe
                (a dataframe of trues and falses), see example.
            value (int, optional):
                value to replace the selected rows.
        """
        self.features = features
        self.selector = selector
        self.value = value
        self.raw_data_frames = None

    def process(self, *data_frames):
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
            if self.selector and self.features:
                filtered_result = self.selector(data_frame)
                data_frame = data_frame.loc[filtered_result, self.features]
            elif self.selector and (not self.features):
                filtered_result = self.selector(data_frame)
                data_frame = data_frame.loc[filtered_result]
            elif (not self.selector) and self.features:
                data_frame = data_frame[self.features]

            if self.value is not None:
                raw_data_frame.loc[data_frame.index, data_frame.columns] = self.value
                data_frame = raw_data_frame

            processed.append(data_frame)

        return processed
