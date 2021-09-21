"""
Operator to aggregate column features based on a column
"""
from tasrif.processing_pipeline import ProcessingOperator


class AggregateOperator(ProcessingOperator):
    """

      Group and aggregate rows in 2D data frame based on a column feature.
      This operator works on a 2D data frames where the
      columns represent the features. The returned data frame contains aggregated values
      as the column features together
      with the base feature used for grouping.

      Examples
      --------

      >>> import pandas as pd
      >>>
      >>> from tasrif.processing_pipeline.custom import AggregateOperator
      >>>
      >>> df0 = pd.DataFrame([['Doha', 25, 30], ['Doha', 17, 50], ['Dubai', 20, 40], ['Dubai', 21, 42]],
      >>>                     columns=['city', 'min_temp', 'max_temp'])
      >>>
      >>> operator = AggregateOperator(
      >>>    groupby_feature_names ="city",
      >>>    aggregation_definition= {"min_temp": ["mean", "std"]})
      >>> df0 = operator.process(df0)
      >>>
      >>> print(df0)
      [    city  min_temp_mean  min_temp_std
      0   Doha           21.0      5.656854
      1  Dubai           20.5      0.707107]

    """
    def __init__(self, groupby_feature_names, aggregation_definition):
        """Creates a new instance of AggregateOperator

        Args:
            groupby_feature_names (str):
                Name of the feature to base the grouping on.
                In case groupby_feature_names includes non string
                such as a function call like pd.Grouper(),
                the column is not shown in the result.
            aggregation_definition (dict):
                Dictionary containing feature to aggregation functions mapping.

        """
        super().__init__()
        self.groupby_feature_names = groupby_feature_names
        self.aggregation_definition = aggregation_definition

    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            pd.DataFrame -or- list[pd.DataFrame]
                Processed dataframe(s) resulting from applying the operator

        """

        columns = self.groupby_feature_names.copy() if isinstance(
            self.groupby_feature_names,
            list) else [self.groupby_feature_names]

        for idx, col in enumerate(columns):
            if not isinstance(col, str):
                del columns[idx]

        for key, value in self.aggregation_definition.items():
            if isinstance(value, list):
                for i in value:
                    columns.append(f'{key}_{i}')
            else:
                columns.append(f'{key}_{value}')

        processed = []
        for data_frame in data_frames:
            data_frame = data_frame.groupby(self.groupby_feature_names,
                                            as_index=False).agg(
                                                self.aggregation_definition)
            data_frame.columns = columns
            processed.append(data_frame)

        return processed
