"""
Operator to aggregate column features based on a column
"""
import pandas as pd

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
      >>> from tasrif.processing_pipeline.custom import LinearFitOperator
      >>>
      >>> df = pd.DataFrame([['001', 25, 30], ['001', 17, 50], ['002', 20, 40], ['002', 21, 42]],
      ...                     columns=['pid', 'min_activity', 'max_activity'])
      >>>
      >>> operator = AggregateOperator(
      ...    groupby_feature_names ="pid",
      ...    aggregation_definition= {"min_temp": ["mean", "std"],
      ...                             "r2,_,intercept": LinearFitOperator(feature_names='min_activity',
      ...                                                                 target='max_activity')})
      >>> df0 = operator.process(df0)
      >>>
      >>> print(df0)
      [   pid  min_activity_mean  min_activity_std   r2     intercept
       0  001               21.0          5.656854  1.0  9.250000e+01
       1  002               20.5          0.707107  1.0  7.105427e-15]

    """
    def __init__(self, groupby_feature_names, aggregation_definition, observers=None):
        """Creates a new instance of AggregateOperator

        Args:
            groupby_feature_names (str):
                Name of the feature to base the grouping on.
                In case groupby_feature_names includes non string
                such as a function call like pd.Grouper(),
                the column is not shown in the result.
            aggregation_definition (dict):
                Dictionary containing feature to aggregation functions mapping.
            observers (list[Observer]):
                Python list of observers

        """
        super().__init__()
        self._observers = []
        self.groupby_feature_names = groupby_feature_names
        self.aggregation_definition = aggregation_definition
        self.pandas_dict = {}
        self.processing_op_dict = {}

        for key, value in self.aggregation_definition.items():
            if isinstance(value, list):
                self.pandas_dict[key] = value
            else:
                if isinstance(value, ProcessingOperator):
                    self.processing_op_dict[key] = value
                else:
                    self.pandas_dict[key] = value

        self.set_observers(observers)

    def set_observers(self, observers):
        if observers and not self._observers:
            self._observers = observers
            for key in self.processing_op_dict:
                self.processing_op_dict[key].set_observers(self._observers)

    def _process_operators(self, groups):
        result = []
        for key, operator in self.processing_op_dict.items():
            data_frame = self._apply_operator(groups, operator)
            column_names = key.split(',')
            if len(data_frame.columns) != len(column_names):
                raise ValueError(f"Length of aggregation key does not match length of aggregated output \
                    {len(data_frame.columns)} != {len(key.split(','))}")

            data_frame.columns = column_names
            columns_filter = data_frame.columns[~data_frame.columns.isin(['_']
                                                                         )]
            data_frame = data_frame[columns_filter]
            result.append(data_frame)

        result = pd.concat(result)
        return result

    @staticmethod
    def _apply_operator(groups, operator):
        """ Applies ProcessingOperator on Pandas DataFrameGroupBy

        Args:
            groups (DataFrameGroupBy):
                GroupBy object from pandas pd.groupby function
            operator (ProcessingOperator):
                operator to be applied on `groups`

        Returns:
            result (pd.DataFrame):
                Dataframe of operator result applied on the groups

        """

        result = []
        for _, group in groups:
            result.append(operator.process(group)[0])

        result = pd.DataFrame(result)
        return result

    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.
        The operator's pseudo code is as follows:
        - Figure out the type of self.aggregation_definition whether it's ProcessingOperator or
          Pandas function
        - Groupby data_frame
        - Apply Pandas agg function using `self.pandas_dict`
        - Apply ProcessingOperator that are in `self.processing_op_dict`
        - Concatenate the previous two dataframes and return the result

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            pd.DataFrame -or- list[pd.DataFrame]
                Processed dataframe(s) resulting from applying the operator

        """

        columns = []
        for key, value in self.pandas_dict.items():
            if isinstance(value, list):
                for i in value:
                    columns.append(f'{key}_{i}')
            else:
                columns.append(f'{key}_{value}')

        processed = []
        for data_frame in data_frames:
            data_frame_group = data_frame.groupby(self.groupby_feature_names,
                                                  as_index=True)

            data_frame = data_frame_group.agg(self.pandas_dict)
            data_frame.columns = columns
            data_frame = data_frame.reset_index()

            if self.processing_op_dict:
                operators_result = self._process_operators(data_frame_group)
                data_frame = pd.concat([data_frame, operators_result], axis=1)

            processed.append(data_frame)

        return processed
