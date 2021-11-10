"""
Operator to visualize participant ID activity per day
"""
import pandas as pd
from tasrif.processing_pipeline import ProcessingOperator


class SetStartHourOfDayOperator(ProcessingOperator):
    """
    Operator to set start hour of the day per participant

    """
    def __init__(  # pylint: disable=too-many-arguments
            self,
            date_feature_name,
            participant_identifier,
            shift=0,
            shifted_date_feature_name='shifted_time_col'):
        """
        Creates a new instance of SetStartHourOfDayOperator. The user will provide the hour using
        `shift`, which will make `date_feature_name` to start on the given hour. The operator will create
        a new column `shifted_date_feature_name`

        Args:
            date_feature_name (str):
                time column
            participant_identifier (str):
                participant identifier
            shift (int):
                shift in hour(s)
            shifted_date_feature_name (str):
                shifted column name to be created

        Examples
        --------

        >>> import numpy as np
        >>> import pandas as pd
        >>> from tasrif.processing_pipeline.custom import SetStartHourOfDayOperator
        >>>
        >>> # Prepare two days for two participants data
        >>> four_days = 48*2
        >>> idx = pd.date_range("2018-01-01", periods=four_days, freq="H", name='startTime')
        >>> activity = np.random.randint(0, 100, four_days)
        >>> df = pd.DataFrame(data=activity, index=idx, columns=['activity'])
        >>> df['participant'] = 1
        >>> df.iloc[48:, 1] = 2
        >>>
        >>>
        >>> operator = SetStartHourOfDayOperator(date_feature_name='startTime',
        ...                                      participant_identifier='participant',
        ...                                      shift=6)
        >>>
        >>> operator.process(df)[0]
            activity    participant     shifted_time_col
        startTime
        2018-01-01 00:00:00     83  1   2017-12-31 18:00:00
        2018-01-01 01:00:00     42  1   2017-12-31 19:00:00
        2018-01-01 02:00:00     79  1   2017-12-31 20:00:00
        2018-01-01 03:00:00     38  1   2017-12-31 21:00:00
        2018-01-01 04:00:00     60  1   2017-12-31 22:00:00
        ...     ...     ...     ...
        2018-01-04 19:00:00     96  2   2018-01-04 13:00:00
        2018-01-04 20:00:00     82  2   2018-01-04 14:00:00
        2018-01-04 21:00:00     74  2   2018-01-04 15:00:00
        2018-01-04 22:00:00     35  2   2018-01-04 16:00:00
        2018-01-04 23:00:00     7   2   2018-01-04 17:00:00

        """
        super().__init__()
        self.date_feature_name = date_feature_name
        self.participant_identifier = participant_identifier
        self.shifted_date_feature_name = shifted_date_feature_name
        self.shift = shift

    def _process(self, *data_frames):  # pylint: disable=R0914
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            data_frames
                Processed data frames

        """
        processed = []
        for data_frame in data_frames:
            if self.date_feature_name in data_frame.columns:
                data_frame = data_frame.set_index(
                    data_frame[self.date_feature_name])
            else:
                assert data_frame.index.name == self.date_feature_name

            data_frame = data_frame.groupby(self.participant_identifier).apply(
                self._change_start_hour_for_day)
            processed.append(data_frame)

        return processed

    def _change_start_hour_for_day(self, data_frame):
        """Changes the start hour of the day to be plotted

        Args:
            data_frame (pd.DataFrame):
              Pandas data_frame to be processed

        Returns:
            data_frame (pd.DataFrame):
              data_frame with changed hour start of the day
        """

        new_datetime = data_frame.index - pd.DateOffset(hours=self.shift)
        data_frame[self.shifted_date_feature_name] = new_datetime

        return data_frame
