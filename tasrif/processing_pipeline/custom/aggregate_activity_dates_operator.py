"""
Operator to shorten a dataframe that has a date column per row per participant.
"""

import pandas as pd

from tasrif.processing_pipeline import ProcessingOperator


class AggregateActivityDatesOperator(ProcessingOperator):
    """
    This method converts the format of a dataframe that has a date column per row per participant.
    The output of this operator will be a dataframe that shows the start date and end date per participant.
    An example scenario would be to 'shorten' the original dataframe to show a summary of when the participant
    started the sleep activity and when the participant ended that activity. This operator assumes that
    `date_feature_name` is sorted.

    The pseudo code of this operator is to group by participantIdentifier, fetch the first date, and end date
    of each group, then return a list of [participantIdentifier, startDate, endDate].
    See example below for clarification.

    >>> import pandas as pd
    >>> import numpy as np
    >>> from tasrif.processing_pipeline.custom import AggregateActivityDatesOperator
    >>>
    >>> df = pd.DataFrame([
    ...     [122,1,'2016-03-13 02:39:00',1],
    ...     [122,1,'2016-03-13 02:40:00',1],
    ...     [122,1,'2016-03-13 02:41:00',1],
    ...     [122,1,'2016-03-13 02:42:00',1],
    ...     [122,1,'2016-03-13 02:43:00',1],
    ...     [122,1,'2016-03-13 02:44:00',1],
    ...     [122,1,'2016-03-13 02:45:00',2],
    ...     [122,1,'2016-03-13 02:46:00',2],
    ...     [122,1,'2016-03-13 02:47:00',1],
    ...     [122,1,'2016-03-13 02:48:00',1],
    ...     [122,2,'2016-03-13 06:06:00',1],
    ...     [122,2,'2016-03-13 06:07:00',1],
    ...     [122,2,'2016-03-13 06:08:00',1],
    ...     [122,2,'2016-03-13 06:09:00',1],
    ...     [122,2,'2016-03-13 06:10:00',1],
    ...     [122,2,'2016-03-13 06:11:00',1],
    ...     [122,2,'2016-03-13 06:12:00',1],
    ...     [122,2,'2016-03-13 06:13:00',1],
    ...     [122,2,'2016-03-13 06:14:00',1],
    ...     [122,2,'2016-03-13 06:15:00',1],
    ...     [144,1,'2016-03-13 06:36:00',1],
    ...     [144,1,'2016-03-13 06:37:00',1],
    ...     [144,1,'2016-03-13 06:38:00',1],
    ...     [144,1,'2016-03-13 06:39:00',1],
    ...     [144,1,'2016-03-13 06:40:00',1],
    ...     [144,1,'2016-03-13 06:41:00',1],
    ...     [144,1,'2016-03-13 06:42:00',1],
    ...     [144,1,'2016-03-13 06:43:00',1],
    ...     [144,1,'2016-03-13 06:44:00',2],
    ...     [144,1,'2016-03-13 06:45:00',1],
    ...     [167,1,'2016-03-14 01:32:00',2],
    ...     [167,1,'2016-03-14 01:33:00',2],
    ...     [167,1,'2016-03-14 01:34:00',1],
    ...     [167,1,'2016-03-14 01:35:00',1],
    ...     [167,1,'2016-03-14 01:36:00',1],
    ...     [167,1,'2016-03-14 01:37:00',1],
    ...     [167,1,'2016-03-14 01:38:00',1],
    ...     [167,1,'2016-03-14 01:39:00',1],
    ...     [167,1,'2016-03-14 01:40:00',1],
    ...     [167,1,'2016-03-14 01:41:00',1],
    ...     [167,2,'2016-03-15 02:36:00',2],
    ...     [167,2,'2016-03-15 02:37:00',2],
    ...     [167,2,'2016-03-15 02:38:00',2],
    ...     [167,2,'2016-03-15 02:39:00',3],
    ...     [167,2,'2016-03-15 02:40:00',3],
    ...     [167,2,'2016-03-15 02:41:00',3],
    ...     [167,2,'2016-03-15 02:42:00',3],
    ...     [167,2,'2016-03-15 02:43:00',3],
    ...     [167,2,'2016-03-15 02:44:00',2],
    ...     [167,2,'2016-03-15 02:45:00',1],
    ...     [167,3,'2016-03-15 03:03:00',1],
    ...     [167,3,'2016-03-15 03:04:00',1],
    ...     [167,3,'2016-03-15 03:05:00',1],
    ...     [167,3,'2016-03-15 03:06:00',1],
    ...     [167,3,'2016-03-15 03:07:00',1],
    ...     [167,3,'2016-03-15 03:08:00',1],
    ...     [167,3,'2016-03-15 03:09:00',1],
    ...     [167,3,'2016-03-15 03:10:00',1],
    ...     [167,3,'2016-03-15 03:11:00',1],
    ...     [167,3,'2016-03-15 03:12:00',1],
    ...     [167,4,'2016-03-15 03:58:00',1],
    ...     [167,4,'2016-03-15 03:59:00',1],
    ...     [167,4,'2016-03-15 04:00:00',1],
    ...     [167,4,'2016-03-15 04:01:00',1],
    ...     [167,4,'2016-03-15 04:02:00',1],
    ...     [167,4,'2016-03-15 04:03:00',1],
    ...     [167,4,'2016-03-15 04:04:00',1],
    ...     [167,4,'2016-03-15 04:05:00',1],
    ...     [167,4,'2016-03-15 04:06:00',1],
    ...     [167,4,'2016-03-15 04:07:00',1],
    ... ], columns=['Id','logId','date','value'])
    >>>
    >>> operator = AggregateActivityDatesOperator(date_feature_name="date",
    ...                                         participant_identifier=['Id', 'logId'],
    ...                                         aggregation_definition={'value': [np.sum,
    ...                                                                           lambda x: x[x == 1].sum()]})
    >>> df = operator.process(df)[0]
    >>> df
    Id  logId   start   end
    0   122     1   2016-03-13 02:39:00     2016-03-13 02:48:00
    1   122     2   2016-03-13 06:06:00     2016-03-13 06:15:00
    2   144     1   2016-03-13 06:36:00     2016-03-13 06:45:00
    3   167     1   2016-03-14 01:32:00     2016-03-14 01:41:00
    4   167     2   2016-03-15 02:36:00     2016-03-15 02:45:00
    5   167     3   2016-03-15 03:03:00     2016-03-15 03:12:00
    6   167     4   2016-03-15 03:58:00     2016-03-15 04:07:00
    """

    def __init__(  # pylint: disable=R0913
        self,
        date_feature_name,
        participant_identifier,
        aggregation_definition=None,
        start_column_name="start",
        end_column_name="end",
    ):
        """Creates a new instance of AggregateActivityDatesOperator

        Args:
            date_feature_name : str
                Name of the feature to identify related timestamp series
            participant_identifier : str or list of str
                Name of the feature(s) identifying the participant
            aggregation_definition : dict
                dict of column names as dict keys and callables as values.
                dict values can be a list of function to be applied on a single column.
                Used to downsample required columns if needed.
            start_column_name : str
                Name of the start column generated by this operator
            end_column_name : str
                Name of the end column generated by this operator
        """
        super().__init__()
        self.date_feature_name = date_feature_name
        self.participant_identifier = participant_identifier
        self.aggregation_definition = aggregation_definition
        self.start_column_name = start_column_name
        self.end_column_name = end_column_name

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
            start_to_end = data_frame.groupby(self.participant_identifier).apply(
                self._show_start_to_end
            )
            start_to_end = start_to_end.reset_index(level=self.participant_identifier)
            start_to_end = start_to_end.reset_index(drop=True)
            processed.append(start_to_end)

        return processed

    def _show_start_to_end(self, dataframe):
        start = dataframe[self.date_feature_name].iloc[0]
        end = dataframe[self.date_feature_name].iloc[-1]
        start_to_end = pd.DataFrame(
            [[start, end]], columns=[self.start_column_name, self.end_column_name]
        )

        if self.aggregation_definition:
            for key, value in self.aggregation_definition.items():
                if isinstance(value, list):
                    for idx, function in enumerate(value):
                        start_to_end[key + "_" + str(idx)] = function(dataframe[key])
                else:
                    start_to_end[key] = value(dataframe[key])

        return start_to_end
