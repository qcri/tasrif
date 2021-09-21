"""
Operator to create a new column feature from existing column features
"""
from tasrif.processing_pipeline import ProcessingOperator


class CreateFeatureOperator(ProcessingOperator):
    """

      Creates a new column based feature from existing features in the data frame.
      This operator works on a 2D data frames where the columns represent the features.
      The defintiion of the new feature is passed as a lambda argument.

      Examples
      --------

      >>> import pandas as pd
      >>>
      >>> from tasrif.processing_pipeline.custom import CreateFeatureOperator
      >>>
      >>> df0 = pd.DataFrame([['tom', 10, 2], ['nick', 15, 2], ['juli', 14, 12]],
      >>>                     columns=['name', 'work_hours', 'off_hours'])
      >>>
      >>> operator = CreateFeatureOperator(
      >>>    feature_name="total_hours",
      >>>    feature_creator=lambda df: df['work_hours'] + df['off_hours'])
      >>> df0 = operator.process(df0)
      >>>
      >>> print(df0)
      [   name  work_hours  off_hours  total_hours
      0   tom          10          2           12
      1  nick          15          2           17
      2  juli          14         12           26]

    """

    def __init__(self, feature_name, feature_creator):
        """Creates a new instance of CreateFeatureOperator

        Args:
            feature_name (str):
                Name of the new feature
            feature_creator (callable):
                lambda operator defining how the new feature can be created from existing column features.

        """
        super().__init__()
        self.feature_name = feature_name
        self.feature_creator = feature_creator


    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            pd.DataFrame -or- list[pd.DataFrame]
                Processed dataframe(s) resulting from applying the operator
        """
        processed = []
        for data_frame in data_frames:
            # result_type is set to 'reduce' so that a Series is always returned.
            data_frame[self.feature_name] = data_frame.apply(self.feature_creator, axis=1, result_type='reduce')
            processed.append(data_frame)

        return processed
