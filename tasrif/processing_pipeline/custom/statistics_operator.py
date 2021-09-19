"""
Operator to aggregate column features based on a column
"""
import pandas as pd

from tasrif.processing_pipeline import ProcessingOperator
from tasrif.processing_pipeline.custom.participation_overview_operator import ParticipationOverviewOperator


class StatisticsOperator(ProcessingOperator):
    """

    Compute statistics of a 2D timeseries dataframe such for each feature and returns the computed statistics
    as a data frame.

    Examples
    --------

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.custom import StatisticsOperator
    >>> df = pd.DataFrame( [
    >>>     ['2020-02-20', 1000, 1800, 1], ['2020-02-21', 5000, 2100, 1], ['2020-02-22', 10000, 2400, 1],
    >>>     ['2020-02-20', 1000, 1800, 1], ['2020-02-21', 5000, 2100, 1], ['2020-02-22', 10000, 2400, 1],
    >>>     ['2020-02-20', 0, 1600, 2], ['2020-02-21', 4000, 2000, 2], ['2020-02-22', 11000, 2400, 2],
    >>>     ['2020-02-20', None, 2000, 3], ['2020-02-21', 0, 2700, 3], ['2020-02-22', 15000, 3100, 3]],
    >>> columns=['Day', 'Steps', 'Calories', 'PersonId'])
    >>>
    >>> filter_features = {
    ...     'Steps': lambda x : x > 0
    ... }
    >>> sop = StatisticsOperator(participant_identifier='PersonId',
    ...                          date_feature_name='Day', filter_features=filter_features)
    >>> sop.process(df)
    [                   statistic         Day       Steps    Calories    PersonId
    0                  row_count          12           9          12          12
    1         missing_data_count           0           1           0           0
    2       duplicate_rows_count           3           3           3           3
    3          participant_count           3           3           3           3
    4                   min_date  2020-02-20  2020-02-20  2020-02-20  2020-02-20
    5                   max_date  2020-02-22  2020-02-22  2020-02-22  2020-02-22
    6                   duration           2           2           2           2
    7  mean_days_per_participant           4           3           4           3
    8  mean_participants_per_day           3           3           4           4]

    """
    def __init__(
            self,
            participant_identifier="Id",
            date_feature_name="Date",
            filter_features=None,
        ):
        """Creates a new instance of StatisticsOperator

        Args:
            participant_identifier (str):
                Name of the feature identifying the participant
            date_feature_name (str):
                Name of the feature identifying the date
            filter_features (dict):
                Dictionary of column/feature name to (lambda) function providing a selection clause.
                Note that if a column or feature name is omitted then a default
                selection of non-zero or non-empty values is applied.
        """
        super().__init__()
        self.participant_identifier = participant_identifier
        self.date_feature_name = date_feature_name
        self.filter_features = filter_features

    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            pd.DataFrame -or- list[pd.DataFrame]
                Processed dataframe(s) resulting from applying the operator
        """

        result = []
        for data_frame in data_frames:
            result.append(self._compute_statistics(data_frame))

        return result

    def _compute_statistics(self, data_frame):

        result = []
        result.append(self._get_row_count(data_frame))
        result.append(self._get_missing_data_count(data_frame))
        result.append(self._get_duplicate_rows_count(data_frame))
        result.append(self._get_participant_count(data_frame))

        dt_data_frame = data_frame.copy()
        dt_feature = self.date_feature_name + "_dt"
        dt_data_frame[dt_feature] = pd.to_datetime(
            dt_data_frame[self.date_feature_name])
        result.extend(self._get_min_max_day(dt_data_frame, dt_feature))

        result.append(
            self._get_overview("participant_vs_features", data_frame,
                               "mean_days_per_participant"))
        result.append(
            self._get_overview("date_vs_features", data_frame,
                               "mean_participants_per_day"))

        return pd.DataFrame.from_records(result)

    def _get_participant_count(self, data_frame):

        columns = list(data_frame.columns)
        count = {"statistic": "participant_count"}
        for column in columns:
            if self.filter_features and column in self.filter_features:
                column_map_func = self.filter_features[column]
                count[column] = data_frame[data_frame.apply(
                    lambda x, func=column_map_func, column=column: func(x[
                        column]),
                    axis=1,
                )][self.participant_identifier].nunique()
            else:
                count[column] = data_frame[data_frame[column].notna()][
                    self.participant_identifier].nunique()

        return count

    def _get_duplicate_rows_count(self, data_frame):

        columns = list(data_frame.columns)
        dup_count = {"statistic": "duplicate_rows_count"}
        for column in columns:
            if self.filter_features and column in self.filter_features:
                column_map_func = self.filter_features[column]
                dup_count[column] = (data_frame[data_frame.apply(
                    lambda x, func=column_map_func, column=column: func(x[
                        column]),
                    axis=1,
                )][[self.date_feature_name,
                    self.participant_identifier]].duplicated().sum())
            else:
                dup_count[column] = (data_frame[data_frame[column].notna()][[
                    self.date_feature_name, self.participant_identifier
                ]].duplicated().sum())

        return dup_count

    # Pylint no-self-use rule is disabled to apply the same style as for in the other
    # functions such as _get_duplicate_rows_count etc..
    def _get_missing_data_count(self, data_frame):  # pylint: disable=no-self-use

        columns = list(data_frame.columns)
        row_count = {"statistic": "missing_data_count"}
        for column in columns:
            row_count[column] = data_frame[
                data_frame[column].isnull()].shape[0]
        return row_count

    def _get_row_count(self, data_frame):

        columns = list(data_frame.columns)
        row_count = {"statistic": "row_count"}
        for column in columns:
            if self.filter_features and column in self.filter_features:
                column_map_func = self.filter_features[column]
                row_count[column] = data_frame[data_frame.apply(
                    lambda x, func=column_map_func, column=column: func(x[
                        column]),
                    axis=1,
                )].shape[0]
            else:
                row_count[column] = data_frame[
                    data_frame[column].notna()].shape[0]

        return row_count

    def _get_min_max_day(self, data_frame, dt_feature):

        columns = list(data_frame.columns)
        min_day = {"statistic": "min_date"}
        max_day = {"statistic": "max_date"}
        duration = {"statistic": "duration"}
        for column in columns:
            if column != dt_feature:
                if self.filter_features and column in self.filter_features:
                    column_map_func = self.filter_features[column]
                    min_day[column] = data_frame[data_frame.apply(
                        lambda x, func=column_map_func, column=column: func(x[
                            column]),
                        axis=1,
                    )][dt_feature].min()
                    max_day[column] = data_frame[data_frame.apply(
                        lambda x, func=column_map_func, column=column: func(x[
                            column]),
                        axis=1,
                    )][dt_feature].max()
                else:
                    min_day[column] = data_frame[
                        data_frame[column].notna()][dt_feature].min()
                    max_day[column] = data_frame[
                        data_frame[column].notna()][dt_feature].max()
                duration[column] = (max_day[column] - min_day[column]).days
                min_day[column] = min_day[column].strftime("%Y-%m-%d")
                max_day[column] = max_day[column].strftime("%Y-%m-%d")

        return [min_day, max_day, duration]

    def _get_overview(self, overview_type, data_frame, statistic_type):

        overview_operator = ParticipationOverviewOperator(
            participant_identifier=self.participant_identifier,
            date_feature_name=self.date_feature_name,
            overview_type=overview_type,
            filter_features=self.filter_features,
        )
        overview = (overview_operator.process(data_frame))[0]
        columns = list(overview.columns)
        aggregation_definition = {}
        for column in columns:
            skip_column = (self.participant_identifier
                           if overview_type == "participant_vs_features" else
                           self.date_feature_name)
            if column != skip_column:
                aggregation_definition[column] = "mean"
            else:
                aggregation_definition[column] = "count"
        aggregation = overview.agg(aggregation_definition)
        result = aggregation.to_dict()
        result["statistic"] = statistic_type

        return result
