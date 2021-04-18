"""
Operator to aggregate column features based on a column
"""
import pandas as pd

from tasrif.processing_pipeline import ProcessingOperator

class AggregateOperator(ProcessingOperator):
    """

      Group and aggregate rows in 2D data frame based on a column feature. This operator works on a 2D data frames where the
      columns represent the features. The returned data frame contains aggregated values as the column features together
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
    """

    def __init__(self, groupby_feature_names, aggregation_definition):
        """Creates a new instance of AggregateOperator

        Parameters
        ----------
        groupby_feature_names : str
            Name of the feature to base the grouping on
        aggregation_definition : dict
            Dictionary containing feature to aggregation functions mapping.
        """
        self.groupby_feature_names = groupby_feature_names
        self.aggregation_definition = aggregation_definition


    def process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Returns
        -------
        pd.DataFrame -or- list[pd.DataFrame]
            Processed dataframe(s) resulting from applying the operator
        """

        columns = self.groupby_feature_names.copy() if isinstance(self.groupby_feature_names, list) else [self.groupby_feature_names]

        # Remove column names that are not string
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
            data_frame = data_frame.groupby(self.groupby_feature_names, as_index=False).agg(self.aggregation_definition)
            data_frame.columns = columns
            processed.append(data_frame)

        return processed
