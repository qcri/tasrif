"""
Operator to slide a fixed length window across a timeseries dataframe
"""
import pandas as pd

from tqdm import tqdm
from tasrif.processing_pipeline import ProcessingOperator


class SlidingWindowOperator(ProcessingOperator):
    """

    From a timeseries dataframe of participants, this function generates two dataframes:
    <time_series_features>, <labels>
    The first dataframe can be used with tsfresh later on,
    while the second has all the labels that we want to predict.

    Notice that the default winsize is 1h and 15 minutres (`1h15t`).
    We use the first hour to extract the features and the 15 min only to collect the ground_truth labels.


      Examples
      --------

        import pandas as pd
        from tasrif.processing_pipeline.custom import SlidingWindowOperator

        >>> df = pd.DataFrame([
        ...    ["2020-02-16 11:45:00",27,102.5],
        ...    ["2020-02-16 12:00:00",27,68.5],
        ...    ["2020-02-16 12:15:00",27,40.0],
        ...    ["2020-02-16 15:15:00",27,282.5],
        ...    ["2020-02-16 15:30:00",27,275.0],
        ...    ["2020-02-16 15:45:00",27,250.0],
        ...    ["2020-02-16 16:00:00",27,235.0],
        ...    ["2020-02-16 16:15:00",27,206.5],
        ...    ["2020-02-16 16:30:00",27,191.0],
        ...    ["2020-02-16 16:45:00",27,166.5],
        ...    ["2020-02-16 17:00:00",27,171.5],
        ...    ["2020-02-16 17:15:00",27,152.0],
        ...    ["2020-02-16 17:30:00",27,124.0],
        ...    ["2020-02-16 17:45:00",27,106.0],
        ...    ["2020-02-16 18:00:00",27,96.5],
        ...    ["2020-02-16 18:15:00",27,86.5],
        ...    ["2020-02-16 17:30:00",31,186.0],
        ...    ["2020-02-16 17:45:00",31,177.0],
        ...    ["2020-02-16 18:00:00",31,171.0],
        ...    ["2020-02-16 18:15:00",31,164.0],
        ...    ["2020-02-16 18:30:00",31,156.0],
        ...    ["2020-02-16 18:45:00",31,157.0],
        ...    ["2020-02-16 19:00:00",31,158.0],
        ...    ["2020-02-16 19:15:00",31,158.5],
        ...    ["2020-02-16 19:30:00",31,150.0],
        ...    ["2020-02-16 19:45:00",31,145.0],
        ...    ["2020-02-16 20:00:00",31,137.0],
        ...    ["2020-02-16 20:15:00",31,141.0],
        ...    ["2020-02-16 20:45:00",31,146.0],
        ...    ["2020-02-16 21:00:00",31,141.0]],
        ...    columns=['dateTime','patientID','CGM'])
        >>> df['dateTime'] = pd.to_datetime(df['dateTime'])
        >>> df
        >>> op = SlidingWindowOperator(winsize="1h15t",
        ...                           time_col="dateTime",
        ...                           label_col="CGM",
        ...                           pid_col="patientID")
        >>> df_timeseries, df_labels, df_label_time, df_pids = op.process(df)[0]
        >>> df_timeseries
        .   dateTime    CGM     seq_id
        0   2020-02-16 15:15:00     282.5   0
        1   2020-02-16 15:30:00     275.0   0
        2   2020-02-16 15:45:00     250.0   0
        3   2020-02-16 16:00:00     235.0   0
        4   2020-02-16 15:30:00     275.0   1
        ...     ...     ...     ...
        143     2020-02-16 19:45:00     145.0   35
        144     2020-02-16 19:15:00     158.5   36
        145     2020-02-16 19:30:00     150.0   36
        146     2020-02-16 19:45:00     145.0   36
        147     2020-02-16 20:00:00     137.0   36
        148 rows × 3 columns

    """
    def __init__(self,  # pylint: disable=too-many-arguments
                 winsize="1h15t",
                 period=15,
                 time_col="time",
                 label_col="CGM",
                 pid_col="patientID"):
        """Creates a new instance of SlidingWindowsOperator

        Args:
            winsize (int, offset):
                Size of the moving window.
                This is the number of observations used for calculating the statistic.
                Each window will be a fixed size.
                If its an offset then this will be the time period of each window.
            period (int):
                periodicity expected between rows. Only used if winsize is an offset
            time_col (str):
                time column in the dataframe
            label_col (str):
                label column in the dataframe
            pid_col (str):
                patient id column in the dataframe

        """
        super().__init__()
        self.winsize = winsize
        self.period = period
        self.time_col = time_col
        self.label_col = label_col
        self.pid_col = pid_col

    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            pd.DataFrame -or- list[pd.DataFrame]
                Processed dataframe(s) resulting from applying the operator
        """

        # Determine window size if self.winsize is an offset
        window_size_in_rows = self.winsize
        if isinstance(self.winsize, str):
            window_size_in_rows = self._determine_window_size()

        processed = []
        for data_frame in data_frames:

            df_labels = []
            df_label_time = []
            df_timeseries = []
            df_pids = []

            last_seq_id = 0

            for pid in tqdm(data_frame["patientID"].unique()):

                last_seq_id, df_ts_tmp, df_label_tmp, df_label_time_tmp, df_pid = self._generate_slide_wins(
                    data_frame[data_frame["patientID"] == pid],
                    start_seq=last_seq_id,
                    max_size=window_size_in_rows
                    )

                df_timeseries.append(df_ts_tmp)
                df_labels.append(df_label_tmp)
                df_label_time.append(df_label_time_tmp)
                df_pids.append(df_pid)

            df_labels = pd.concat(df_labels).reset_index(drop=True)
            df_label_time = pd.concat(df_label_time).reset_index(drop=True)
            df_timeseries = pd.concat(df_timeseries).reset_index(drop=True)

            df_pids = pd.concat(df_pids).reset_index(drop=True)
            df_pids.name = "pid"

            processed.append(
                [df_timeseries, df_labels, df_label_time, df_pids])

        return processed

    def _generate_slide_wins(self, df_in, start_seq=0, max_size=5):
        """

        The following code will construct a rolling win that could be based on either time or #win
        This will feed list_of_indexes with the sub-win indices that will be used in the next for loop

        Time-based win might be smaller than the expected size. We fix it by comparing the size of each
        value in the list_of_indices with the size of the last element

        Args:
            df_in (pd.DataFrame):
                Pandas DataFrame that has been filtered by `_process_epoch`
            start_seq (int):
                sequence id
            max_size:
                window size to keep for the returned data

        Returns:
            seq_id (int):
                last sequence id
            transformed_df (pd.DataFrame):
                Dataframe that contains the windows labeled by sequence ids
            lables (pd.Series):
                Series of ground truths of self.label_col
            label_times (pd.Series):
                Series of ground truths of datetime
            pid (pd.Series):
                Series of patient ID that belong to transformed_df

        Raises:
            ValueError: Occurs when _generate_slide_wins is called for more than one pid

        """

        seq_id = start_seq
        transformed_df = []
        list_of_indices = []
        labels = []
        label_times = []

        pid = df_in[self.pid_col].unique()
        if len(pid) > 1:
            raise ValueError(
                "_generate_slide_wins must be called with one pid")

        pid = pid[0]
        dataframe = df_in.reset_index(drop=True).copy()


        dataframe.reset_index() \
            .rolling(self.winsize, on=self.time_col, center=False)["index"] \
            .apply((lambda x: list_of_indices.append(x.tolist()) or 0))

        # Append label index to labels list
        for idx in list_of_indices:
            if len(idx) != max_size:
                continue

            labels.append(dataframe.loc[idx].iloc[-1][self.label_col])
            label_times.append(dataframe.loc[idx].iloc[-1][self.time_col])

            tmp_df = dataframe.loc[idx[0:-1]].copy()
            tmp_df["seq_id"] = seq_id
            seq_id += 1

            del tmp_df[self.pid_col]

            transformed_df.append(tmp_df)

        labels = pd.Series(labels)
        labels.name = "ground_truth"

        label_times = pd.Series(label_times)
        label_times.name = "gt_time"

        transformed_df = pd.concat(transformed_df).reset_index(drop=True)
        pid = pd.Series([pid] * labels.shape[0])
        pid.name = "pid"

        return seq_id, transformed_df, labels, label_times, pid

    def _determine_window_size(self):
        """
        method to determine size of sliding window in number of rows

        Returns:
            window_size_in_rows (int):
                size of window

        """

        today = pd.to_datetime("today")
        time1 = pd.Timestamp(today.date())
        time2 = today.date() + pd.tseries.frequencies.to_offset(self.winsize)

        time_delta = time2 - time1
        time_delta = time_delta.seconds // 60

        window_size_in_rows = time_delta // self.period

        return window_size_in_rows
