"""
Operator to convert a continuous variable to a categorical variable, useful for binning data
"""
import pandas as pd
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin


class CutOperator(InputsAreDataFramesValidatorMixin, PandasOperator):
    """

      Bin values into discrete intervals using Pandas ``cut``

      Examples
      --------

        >>> import pandas as pd
        >>> import numpy as np
        >>> import datetime
        >>> from tasrif.processing_pipeline.pandas import CutOperator
        >>>
        >>>
        >>> df = pd.DataFrame({
        ...         'Time': pd.date_range('2018-01-01', '2018-01-10', freq='1H', closed='left'),
        ...         'Steps': np.random.randint(100,5000, size=9*24),
        ...         }
        ...      )
        >>>
        >>> ids = []
        >>> for i in range(1, 217):
        ...     ids.append(i%10 + 1)
        >>>
        >>> df["Id"] = ids
        >>> df
        ### input ###
        Time    Steps   Id
        0   2018-01-01 00:00:00     1554    2
        1   2018-01-01 01:00:00     1583    3
        2   2018-01-01 02:00:00     1540    4
        3   2018-01-01 03:00:00     4760    5
        4   2018-01-01 04:00:00     1671    6
        ...     ...     ...     ...
        211     2018-01-09 19:00:00     298     3
        212     2018-01-09 20:00:00     1059    4
        213     2018-01-09 21:00:00     556     5
        214     2018-01-09 22:00:00     3021    6
        215     2018-01-09 23:00:00     4449    7

        >>> # 4 Equal width bins
        >>> df1 = df.copy()
        >>> operator = CutOperator(cut_column_name='Steps',
        ...                        bin_column_name='Bin',
        ...                        bins=4,
        ...                        retbins=True)
        >>>
        >>> df1, bins = operator.process(df1)[0]
        >>> print('Bins:', bins)
        >>> df1
        ### output 1 ###
        Bins: [ 147.178 1357.5   2563.    3768.5   4974.   ]
            Time    Steps   Id  Bin
        0   2018-01-01 00:00:00     3911    2   (3768.5, 4974.0]
        1   2018-01-01 01:00:00     360     3   (147.178, 1357.5]
        2   2018-01-01 02:00:00     4466    4   (3768.5, 4974.0]
        3   2018-01-01 03:00:00     1983    5   (1357.5, 2563.0]
        4   2018-01-01 04:00:00     3059    6   (2563.0, 3768.5]
        ...     ...     ...     ...     ...
        211     2018-01-09 19:00:00     4387    3   (3768.5, 4974.0]
        212     2018-01-09 20:00:00     1679    4   (1357.5, 2563.0]
        213     2018-01-09 21:00:00     2445    5   (1357.5, 2563.0]
        214     2018-01-09 22:00:00     2028    6   (1357.5, 2563.0]
        215     2018-01-09 23:00:00     268     7   (147.178, 1357.5]

        >>> # Custom bins
        >>> cut_labels = ['Sedentary', "Light", 'Moderate', 'Vigorous']
        >>> cut_bins =[0, 500, 2000, 6000, float("inf")]
        >>>
        >>> df2 = df.copy()
        >>> operator = CutOperator(cut_column_name='Steps',
        >>>                        bin_column_name='Bin',
        >>>                        bins=cut_bins,
        >>>                        labels=cut_labels)
        >>>
        >>> df2 = operator.process(df1)[0]
        >>> print(df2['Bin'].value_counts())
        >>> df2
        ### Output 2 ###
        Moderate     135
        Light         64
        Sedentary     17
        Vigorous       0
        Name: Bin, dtype: int64
            Time    Steps   Id  Bin
        0   2018-01-01 00:00:00     3911    2   Moderate
        1   2018-01-01 01:00:00     360     3   Sedentary
        2   2018-01-01 02:00:00     4466    4   Moderate
        3   2018-01-01 03:00:00     1983    5   Light
        4   2018-01-01 04:00:00     3059    6   Moderate
        ...     ...     ...     ...     ...
        211     2018-01-09 19:00:00     4387    3   Moderate
        212     2018-01-09 20:00:00     1679    4   Light
        213     2018-01-09 21:00:00     2445    5   Moderate
        214     2018-01-09 22:00:00     2028    6   Moderate
        215     2018-01-09 23:00:00     268     7   Sedentary

    """
    def __init__(self, cut_column_name, bin_column_name, bins, **kwargs):
        """Initializes the operator

        Args:
            cut_column_name (str):
                Name of the column to perform the cut operation on
            bin_column_name (str):
                Name of the column representing the bins
            bins (int, sequence of scalars, or IntervalIndex):
                - **int** : Defines the number of equal-width bins in the range of x.
                  The range of x is extended by .1% on each side to
                  include the minimum and maximum values of x.
                - **sequence of scalars** : Defines the bin edges allowing for non-uniform width.
                  No extension of the range of x is done.
                - **IntervalIndex** : Defines the exact bins to be used.
                  Note that IntervalIndex for bins must be non-overlapping.
            **kwargs:
              key word arguments passed to pandas ``cut`` method

        """
        self.cut_column_name = cut_column_name
        self.bin_column_name = bin_column_name
        self.bins = bins
        super().__init__(kwargs)
        self.kwargs = kwargs

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
            if 'retbins' in self.kwargs:
                result, bins = pd.cut(data_frame[self.cut_column_name],
                                      self.bins, **self.kwargs)

                data_frame[self.bin_column_name] = result
                processed.append((data_frame, bins))
            else:
                data_frame[self.bin_column_name] = pd.cut(
                    data_frame[self.cut_column_name], self.bins, **self.kwargs)
                processed.append(data_frame)
        return processed
