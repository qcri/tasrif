"""
Operator to aggregate column features based on a column
"""
from tasrif.processing_pipeline import ProcessingOperator


class ParticipationOverviewOperator(ProcessingOperator):
    """

    Creates a dataframe showing the overview of a dataframe representing data
    collected from people over several days.

    Specifically two types of overviews are generated:
        - **partipant_vs_features**: This overview creates a dataframe where each cell
          (where row identifies the participant and column identifies the feature)
          is assigned a value corresponding to the number of days
          for which data is available (or was measured) for this participant

        - **date_vs_features**: This overview creates a dataframe where each cell
          (where row identifies the date and column identifies the feature)
          is assigned a value corresponding to the number of participants
          for which data is available (or was measured) for this day.

    Examples
    --------

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.custom import ParticipationOverviewOperator
    >>> df = pd.DataFrame( [
    ...     ['2020-02-20', 1000, 1800, 1], ['2020-02-21', 5000, 2100, 1], ['2020-02-22', 10000, 2400, 1],
    ...     ['2020-02-20', 0, 1600, 2], ['2020-02-21', 4000, 2000, 2], ['2020-02-22', 11000, 2400, 2],
    ...     ['2020-02-20', 500, 2000, 3], ['2020-02-21', 0, 2700, 3], ['2020-02-22', 15000, 3100, 3]],
    ...     columns=['Day', 'Steps', 'Calories', 'PersonId'])
    >>>
    >>> op = ParticipationOverviewOperator(participant_identifier='PersonId', date_feature_name='Day')
    >>> df1 = op.process(df)
    >>> df1
    [   PersonId  Count  Steps  Calories
    0         1      3      3         3
    1         2      3      2         3
    2         3      3      2         3]

    >>> op2 = ParticipationOverviewOperator(participant_identifier='PersonId',
    ...                                     date_feature_name='Day', overview_type='date_vs_features')
    >>> df2 = op2.process(df)
    >>> df2
    [          Day  Steps  Calories  Count
    0  2020-02-20      2         3      3
    1  2020-02-21      2         3      3
    2  2020-02-22      3         3      3]

    >>> # Count only days where the number of steps > 1000
    >>> od = {
    ...     'Steps': lambda x: x > 1000
    ... }
    >>> op3 = ParticipationOverviewOperator(participant_identifier='PersonId',
    >>>                                     date_feature_name='Day', filter_features=od)
    >>> df3 = op3.process(df)
    >>> df3
    [   PersonId  Count  Steps  Calories
    0         1      3      2         3
    1         2      3      2         3
    2         3      3      1         3]

    >>> # Count only days where the number of steps > 1000
    >>>
    >>> op4 = ParticipationOverviewOperator(participant_identifier='PersonId',
    ...                                     date_feature_name='Day',
    ...                                     overview_type='date_vs_features', filter_features=od)
    >>>
    >>> df4 = op4.process(df)
    >>> df4
    [          Day  Steps  Calories  Count
    0  2020-02-20      0         3      3
    1  2020-02-21      2         3      3
    2  2020-02-22      3         3      3]

    """
    def __init__(self,
                 participant_identifier="Id",
                 date_feature_name="Date",
                 overview_type="participant_vs_features",
                 filter_features=None):
        """Creates a new instance of ParticipationOverviewOperator

        Args:
            participant_identifier (str):
                Name of the feature identifying the participant
            date_feature_name (str):
                Name of the feature identifying the date
            overview_type (str):
                Type of overview which can take one of the two values  `participant_vs_features` or `date_vs_features`
            filter_features (dict):
                Dictionary of column/feature name to (lambda) function providing a selection clause.
                Note that if a column or feature name is omitted
                then a default selection of non-zero or non-empty values is applied.

        """
        super().__init__()
        self.participant_identifier = participant_identifier
        self.date_feature_name = date_feature_name
        self.filter_features = filter_features
        self.overview_type = overview_type

    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            pd.DataFrame -or- list[pd.DataFrame]
                Processed dataframe(s) resulting from applying the operator

        Raises:
            ValueError: unknown `self.overview_type`.
        """

        if self.overview_type == "date_vs_features":
            return self._create_overview(self.date_feature_name,
                                         self.participant_identifier,
                                         *data_frames)
        if self.overview_type == "participant_vs_features":
            return self._create_overview(self.participant_identifier,
                                         self.date_feature_name, *data_frames)
        raise ValueError(f"Unknown type of overview: {self.overview_type}")

    def _create_overview(self, group_by_feature, counted_feature,
                         *data_frames):

        count_non_na = lambda x: x.notna().sum()

        processed = []

        for data_frame in data_frames:
            columns = list(data_frame.columns)
            filter_features = {}
            for column in columns:
                if column == counted_feature:
                    filter_features[counted_feature] = (
                        counted_feature, lambda x: x.notna().sum())
                elif column == group_by_feature:
                    pass
                else:
                    if self.filter_features and column in self.filter_features:
                        column_map_func = self.filter_features[column]
                        filter_features[column] = (
                            column,
                            lambda x, func=column_map_func: func(x).sum())
                    else:
                        filter_features[column] = (column, count_non_na)
            data_frame = data_frame.groupby(
                group_by_feature, as_index=False).agg(**filter_features)
            processed.append(data_frame)

        return processed
