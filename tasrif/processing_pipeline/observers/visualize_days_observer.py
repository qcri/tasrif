"""Module that defines the VisuzlizeDays class
"""

import calendar
from datetime import timedelta

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import dates
from matplotlib.colors import LinearSegmentedColormap
from matplotlib import cm

from tasrif.processing_pipeline.observers.functional_observer import FunctionalObserver

class VisualizeDaysObserver(FunctionalObserver):  # pylint: disable=R0902
    """DataprepObserver class to create a report for a dataframe
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        date_feature_name,
        signals,
        participant_identifier,
        participant_filter=-1,
        signals_as_area=None,
        start_hour_col=None,
        end_date_feature_name=None,
        granularity='T',
        log_scale=False,
        figsize=(7, 4)):
        """
        VisualizeDaysObserver constructor

        Args:
            date_feature_name (str):
                time column
            signals (str, list):
                name of column(s) to plot as a line plot
            participant_identifier (str):
                participant identifier column
            participant_filter (int, list):
                participants to plot. Can be one of the following values
                - -1 (default), to plot the days of the first participant in the dataframe
                - -2, to plot all participants days
                - list of values in `participant_identifier`, to plot specific participants days
            signals_as_area (str, list):
                name of column(s) to plot as area plot. Columns have to be of boolean type.
            start_hour_col (str):
                optional parameter to draw the days starting from the given hour. Default is to draw
                from midnight-to-midnight. This column can be created using `SetStartHourOfDayOperator`
            end_date_feature_name (str):
                Optional column time name that represents the end time of the activity period
            granularity (str):
                Used when `end_date_feature_name` is set. Represents the rounding of start and end
                date_feature_name columns. Default is `T` which is 1 minute
            log_scale (bool):
                whether to draw y-axis in log-scale
            figsize (tuple):
                figure size

        Examples
        --------

        >>> import numpy as np
        >>> import pandas as pd
        >>> from tasrif.processing_pipeline import SequenceOperator, NoopOperator
        >>> from tasrif.processing_pipeline.custom import VisualizeDaysOperator, SetStartHourOfDayOperator
        >>> from tasrif.processing_pipeline.observers import VisualizeDaysObserver
        >>> from tasrif.processing_pipeline.pandas import FillNAOperator
        >>>
        >>> def generate_days(periods, freq, participant=1, start_day="2018-01-01", name='startTime'):
        ...     idx = pd.date_range(start_day, periods=periods, freq=freq, name='startTime')
        ...     activity = np.random.randint(0, 100, periods)
        ...     df = pd.DataFrame(data=activity, index=idx, columns=['activity'])
        ...     df['steps'] = np.random.randint(100, 1000, periods)
        ...     df['participant'] = participant
        ...     return df
        >>>
        >>> def generate_sleep(df, start_time='23:30', end_time='8:00', name='sleep'):
        ...     df[name] = False
        ...     time_filter = df.between_time(start_time=start_time, end_time=end_time).index
        ...     df.loc[time_filter, name] = True
        ...     df['not_' + name] = ~df[name]
        ...
        ...     # reduce activity between 23:30 - 08:00
        ...     df.loc[time_filter, 'activity'] = df.loc[time_filter, 'activity'] / 50
        ...     df.loc[time_filter, 'steps'] = 0
        ...     return df
        >>>
        >>> def generate_data(participants=2, days=2):
        ...     dfs = []
        ...     for i in range(participants):
        ...         df = generate_days(periods=24*days, freq='H', participant=i)
        ...         df = generate_sleep(df)
        ...         dfs.append(df)
        ...     return pd.concat(dfs)
        >>>
        >>> df = generate_data()
        >>>
        >>> # Add None to activity first day for participant 0
        >>> df.iloc[36:48, 0] = None
        >>> df
                    activity    steps   participant     sleep   not_sleep
        startTime
        2018-01-01 00:00:00     0.42    0   0   True    False
        2018-01-01 01:00:00     0.70    0   0   True    False
        2018-01-01 02:00:00     0.08    0   0   True    False
        2018-01-01 03:00:00     0.00    0   0   True    False
        2018-01-01 04:00:00     0.92    0   0   True    False
        ...     ...     ...     ...     ...     ...
        2018-01-02 19:00:00     90.00   121     1   False   True
        2018-01-02 20:00:00     48.00   312     1   False   True
        2018-01-02 21:00:00     57.00   303     1   False   True
        2018-01-02 22:00:00     76.00   916     1   False   True
        2018-01-02 23:00:00     55.00   474     1   False   True

        >>> # With no shift
        >>> observer = VisualizeDaysObserver(date_feature_name='startTime',
        ...                                  signals=['activity', 'steps'],
        ...                                  participant_identifier='participant',
        ...                                  signals_as_area=['sleep'])
        >>>
        >>> pipeline = SequenceOperator([NoopOperator()], observers=[observer])
        >>> pipeline.process(df)[0]
            activity    steps   participant     sleep   not_sleep
        startTime
        2018-01-01 00:00:00     1.56    0   0   True    False
        2018-01-01 01:00:00     0.64    0   0   True    False
        2018-01-01 02:00:00     1.72    0   0   True    False
        2018-01-01 03:00:00     1.08    0   0   True    False
        2018-01-01 04:00:00     1.70    0   0   True    False
        ...     ...     ...     ...     ...     ...
        2018-01-02 19:00:00     16.00   805     1   False   True
        2018-01-02 20:00:00     61.00   566     1   False   True
        2018-01-02 21:00:00     48.00   895     1   False   True
        2018-01-02 22:00:00     68.00   818     1   False   True
        2018-01-02 23:00:00     23.00   883     1   False   True

        >>> # With shift
        >>> observer = VisualizeDaysObserver(date_feature_name='startTime',
        ...                                 signals=['activity', 'steps'],
        ...                                 participant_identifier='participant',
        ...                                 signals_as_area=['sleep'],
        ...                                 start_hour_col='shifted_time_col')
        >>>
        >>>
        >>> pipeline = SequenceOperator([
        ...      SetStartHourOfDayOperator(date_feature_name='startTime',
        ...                                participant_identifier='participant',
        ...                                shifted_date_feature_name='shifted_time_col',
        ...                                shift=6),
        ...      FillNAOperator(value=300),
        ... ], observers=[observer])
        >>>
        >>>
        >>> pipeline.process(df)[0]
            activity    steps   participant     sleep   not_sleep   shifted_time_col
        startTime
        2018-01-01 00:00:00     1.56    0   0   True    False   2017-12-31 18:00:00
        2018-01-01 01:00:00     0.64    0   0   True    False   2017-12-31 19:00:00
        2018-01-01 02:00:00     1.72    0   0   True    False   2017-12-31 20:00:00
        2018-01-01 03:00:00     1.08    0   0   True    False   2017-12-31 21:00:00
        2018-01-01 04:00:00     1.70    0   0   True    False   2017-12-31 22:00:00
        ...     ...     ...     ...     ...     ...     ...
        2018-01-02 19:00:00     16.00   805     1   False   True    2018-01-02 13:00:00
        2018-01-02 20:00:00     61.00   566     1   False   True    2018-01-02 14:00:00
        2018-01-02 21:00:00     48.00   895     1   False   True    2018-01-02 15:00:00
        2018-01-02 22:00:00     68.00   818     1   False   True    2018-01-02 16:00:00
        2018-01-02 23:00:00     23.00   883     1   False   True    2018-01-02 17:00:00

        """
        self.date_feature_name = date_feature_name
        self.signals = signals
        self.signals_as_area = signals_as_area
        self.participant_identifier = participant_identifier
        self.participant_filter = participant_filter
        self.start_hour_col = start_hour_col
        self.end_date_feature_name = end_date_feature_name
        self.granularity = granularity
        self.log_scale = log_scale
        self.figsize = figsize

        if isinstance(self.signals, str):
            self.signals = [self.signals]

        if isinstance(self.signals_as_area, str):
            self.signals_as_area = [self.signals_as_area]

    def _observe(self, operator, *data_frames):
        """
        Observe the passed data using the processing configuration specified
        in the constructor

        Args:
            operator (ProcessingOperator):
                Processing operator which is observed
            *data_frames (list of pd.DataFrame):
                Variable number of pandas dataframes to be observed

        """
        for data_frame in data_frames:
            df_plot = data_frame[0]

            if self.date_feature_name in df_plot.columns:
                df_plot = df_plot.set_index(
                    df_plot[self.date_feature_name])
            else:
                assert df_plot.index.name == self.date_feature_name

            if self.participant_filter == -1:
                first_participant = df_plot[self.participant_identifier].iloc[0]
                self.participant_filter = [first_participant]


            if self.participant_filter != -2:
                df_plot_filter = df_plot[self.participant_identifier].isin(
                    self.participant_filter)

                df_plot = df_plot[df_plot_filter]

            df_plot.groupby(self.participant_identifier).apply(self._plot_days)

    def _plot_days(self, df_plot): # pylint: disable=R0914,R0912,R0915,R0913
        """plots days of participant in df_plot

        Args:
            df_plot (pd.DataFrame):
              Pandas data_frame to be processed

        """
        if self.start_hour_col:
            dfs_per_group = [
                pd.DataFrame(group[1])
                for group in df_plot.groupby(df_plot[self.start_hour_col].dt.date)
            ]
        else:
            dfs_per_group = [
                pd.DataFrame(group[1])
                for group in df_plot.groupby(df_plot.index.date)
            ]

        fig, ax1 = plt.subplots(len(dfs_per_group),
                                1,
                                figsize=self.figsize)

        if len(dfs_per_group) == 1:
            ax1 = [ax1]

        for idx, df_panel in enumerate(dfs_per_group):

            maxy = 0
            colors = ["black", "forestgreen", "honeydew", "palegreen"]
            colors = self._get_bar_colors(len(self.signals))
            for i, signal in enumerate(self.signals):

                if self.log_scale:
                    df_panel[signal] = np.log(df_panel[signal])

                if self.end_date_feature_name:
                    self._draw_bar(df_panel,
                                   signal,
                                   ax1[idx],
                                   colors[i],
                                   colors[i])
                else:
                    ax1[idx].plot(df_panel.index,
                                  df_panel[signal],
                                  label=signal,
                                  linewidth=2,
                                  color=colors[i],
                                  alpha=1)

                if df_panel[signal].max() > maxy:
                    maxy = df_panel[signal].max()


            if self.signals_as_area:
                facecolors = self._get_area_colors(len(self.signals_as_area))
                endy = 0
                for i, signal_area in enumerate(self.signals_as_area):
                    starty = endy
                    endy = endy + (maxy / len(self.signals_as_area))
                    signal_is_true = df_panel[signal_area]
                    ax1[idx].fill_between(df_panel.index,
                                          starty,
                                          endy,
                                          where=signal_is_true,
                                          facecolor=facecolors[i],
                                          alpha=0.7,
                                          label=signal_area)

            ax1[idx].margins(0.1)
            ax1[idx].tick_params(axis='x',
                                 which='both',
                                 bottom=False,
                                 top=False,
                                 labelbottom=True,
                                 rotation=0)
            ax1[idx].set_facecolor('snow')


            if self.start_hour_col:
                shift = df_panel.index[0] - df_panel[self.start_hour_col].iloc[0]
                shift = shift.seconds // (60*60)
                new_start_datetime = df_panel.index[0] - timedelta(
                    hours=(df_panel.index[0].hour - shift) % 24,
                    minutes=df_panel.index[0].minute,
                    seconds=df_panel.index[0].second)

                new_end_datetime = df_panel.index[0] - timedelta(
                    hours=(df_panel.index[0].hour - shift) % 24,
                    minutes=df_panel.index[0].minute,
                    seconds=df_panel.index[0].second) + timedelta(minutes=1439)

            else:
                new_start_datetime = df_panel.index[0]
                new_end_datetime = df_panel.index[0] + timedelta(minutes=1439)


            new_start_datetime = pd.to_datetime(new_start_datetime)
            new_end_datetime = pd.to_datetime(new_end_datetime)

            ax1[idx].set_xlim(new_start_datetime, new_end_datetime)
            ax1[idx].set_ylim(0, maxy)

            y_label = self._get_day_label(df_panel)
            ax1[idx].set_ylabel(f"{y_label}",
                                rotation=0,
                                horizontalalignment="right",
                                verticalalignment="center")

            ax1[idx].set_xticks([])
            ax1[idx].yaxis.set_tick_params(labelsize=8)

        participant = df_plot.iloc[0][self.participant_identifier]
        ax1[0].set_title(f"PID = {str(participant)}", fontsize=16)

        ax1[-1].set_xlabel('Time')
        ax1[-1].xaxis.set_minor_locator(
            dates.HourLocator(interval=4))  # every 4 hours
        ax1[-1].xaxis.set_minor_formatter(
            dates.DateFormatter('%H:%M'))  # hours and minutes


        handles, labels = ax1[-1].get_legend_handles_labels()
        fig.legend(handles,
                   labels,
                   loc='lower right',
                   ncol=1,
                   fontsize=8,
                   shadow=True)

        plt.show()
        plt.close()

    def _draw_bar(self, data_frame, signal, axes, color, edgecolor): # pylint: disable=R0913
        """Called when `end_datetime_feature` is set. Draws bargraph
        where each bar has a height of signal, and a width of
        `end_datetime_feature` - `date_time_feature`.

        Args:
            data_frame (pd.DataFrame):
              Pandas data_frame to be processed
            signal (str):
              signal to plot
            axes (matplotlib.axes):
              axes to plot in
            color (str):
              matplotlib color
            edgecolor (str):
              matplotlib color

        """
        data = data_frame.index
        data = data.round(self.granularity)

        end_time = data_frame[self.end_date_feature_name]
        end_time = end_time.dt.round(self.granularity)

        width = dates.date2num(end_time) - dates.date2num(data)
        axes.bar(x=data,
                 height=data_frame[signal],
                 width=width,
                 color=color,
                 edgecolor=edgecolor,
                 align='edge')


    @staticmethod
    def _get_day_label(data_frame):
        """formats ylabel for the day given `data_frame`

        Args:
            data_frame (pd.DataFrame):
              Pandas data_frame to be processed

        Returns:
            string (str):
              day in month - month - day

        """
        string = ""
        startdate = data_frame.index[0]
        enddate = data_frame.index[-1]

        if startdate.day == enddate.day:
            string = (f"{startdate.day} - "
                f"{calendar.month_name[startdate.month][:3]}\n"
                f"{calendar.day_name[startdate.dayofweek]}")

        else:
            if startdate.month == enddate.month:
                string = (f"{startdate.day}/{enddate.day} - "
                    f"{calendar.month_name[startdate.month][:3]}\n"
                    f"{calendar.day_name[startdate.dayofweek][:3]}/"
                    f"{calendar.day_name[enddate.dayofweek][:3]}")
            else:
                string = (f"{startdate.day} - "
                    f"{calendar.month_name[startdate.month][:3]}/{enddate.day} - "
                    f"{calendar.month_name[enddate.month][:3]}\n "
                    f"{calendar.day_name[startdate.dayofweek][:3]}/"
                    f"{calendar.day_name[enddate.dayofweek][:3]}")

        return string

    @staticmethod
    def _get_bar_colors(num_signals):
        """Returns colors for bargraphs such as number of steps.

        Args:
            num_signals (int):
              Number of signals to plot

        Returns:
            list:
              list of matplotlib.colors

        """
        colors = ["black", "forestgreen", "honeydew", "palegreen"]
        cmap1 = LinearSegmentedColormap.from_list("mycmap", colors)
        cmap = cm.get_cmap(cmap1, num_signals)
        color_indices = np.linspace(0.0, 1.0, num_signals, endpoint=False)
        return [cmap(c) for c in color_indices]

    @staticmethod
    def _get_area_colors(num_areas):
        """Returns colors for areas such as sleep period.

        Args:
            num_areas (int):
              Number of areas to plot

        Returns:
            list:
              list of matplotlib.colors

        """
        colors = ['royalblue', 'green', 'orange']
        cmap1 = LinearSegmentedColormap.from_list("mycmap", colors)
        cmap = cm.get_cmap(cmap1, num_areas)
        color_indices = np.linspace(0.0, 1.0, num_areas, endpoint=False)
        return [cmap(c) for c in color_indices]
