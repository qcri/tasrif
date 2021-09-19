"""
Operator to convert a continuous variable to a categorical variable, useful for binning data
"""
import pandas as pd
from tasrif.processing_pipeline import PandasOperator
from tasrif.processing_pipeline.validators import InputsAreDataFramesValidatorMixin


class QCutOperator(InputsAreDataFramesValidatorMixin, PandasOperator):
    """

      Quantile-based discretization function using Pandas ``qcut``

      Examples
      --------

        >>> import pandas as pd
        >>> import numpy as np
        >>> import datetime
        >>>
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
        ### Input ###
        Time    Steps   Id
        0   2018-01-01 00:00:00     4974    2
        1   2018-01-01 01:00:00     3377    3
        2   2018-01-01 02:00:00     293     4
        3   2018-01-01 03:00:00     3389    5
        4   2018-01-01 04:00:00     1906    6
        ...     ...     ...     ...
        211     2018-01-09 19:00:00     4715    3
        212     2018-01-09 20:00:00     1947    4
        213     2018-01-09 21:00:00     2181    5
        214     2018-01-09 22:00:00     2701    6
        215     2018-01-09 23:00:00     3444    7

        >>> # 4 Equally distributed bins
        >>> df1 = df.copy()
        >>> operator = QCutOperator(cut_column_name='Steps',
        ...                         bin_column_name='Bin',
        ...                         quantile=4,
        ...                         retbins=True)
        >>> df1, bins = operator.process(df1)[0]
        >>> print('Bins:', bins)
        >>> df1
        ### Output 1 ###
        Bins: [ 100.   1341.5  2437.5  3502.25 4987.  ]
        (99.999, 1341.5]     54
        (1341.5, 2437.5]     54
        (2437.5, 3502.25]    54
        (3502.25, 4987.0]    54
        Name: Bin, dtype: int64
            Time    Steps   Id  Bin
        0   2018-01-01 00:00:00     1414    2   (1341.5, 2437.5]
        1   2018-01-01 01:00:00     1513    3   (1341.5, 2437.5]
        2   2018-01-01 02:00:00     937     4   (99.999, 1341.5]
        3   2018-01-01 03:00:00     3551    5   (3502.25, 4987.0]
        4   2018-01-01 04:00:00     2573    6   (2437.5, 3502.25]
        ...     ...     ...     ...     ...
        211     2018-01-09 19:00:00     2835    3   (2437.5, 3502.25]
        212     2018-01-09 20:00:00     409     4   (99.999, 1341.5]
        213     2018-01-09 21:00:00     691     5   (99.999, 1341.5]
        214     2018-01-09 22:00:00     1533    6   (1341.5, 2437.5]
        215     2018-01-09 23:00:00     3018    7   (2437.5, 3502.25]

        >>> # Custom bins
        >>> cut_labels = ['Sedentary', "Light", 'Moderate', 'Vigorous']
        >>> quantiles = [0, 0.2, 0.5, 0.80, 1]
        >>>
        >>> df2 = df.copy()
        >>> operator = QCutOperator(cut_column_name='Steps',
        ...                         bin_column_name='Bin',
        ...                         quantile=quantiles,
        ...                         labels=cut_labels)
        >>> df2 = operator.process(df1)[0]
        >>> print(df2['Bin'].value_counts())
        >>> df2
        ### Output 2 ###
        Moderate     65
        Light        64
        Sedentary    44
        Vigorous     43
        Name: Bin, dtype: int64
        ...
            Time    Steps   Id  Bin
        0   2018-01-01 00:00:00     1414    2   Light
        1   2018-01-01 01:00:00     1513    3   Light
        2   2018-01-01 02:00:00     937     4   Sedentary
        3   2018-01-01 03:00:00     3551    5   Moderate
        4   2018-01-01 04:00:00     2573    6   Moderate
        ...     ...     ...     ...     ...
        211     2018-01-09 19:00:00     2835    3   Moderate
        212     2018-01-09 20:00:00     409     4   Sedentary
        213     2018-01-09 21:00:00     691     5   Sedentary
        214     2018-01-09 22:00:00     1533    6   Light
        215     2018-01-09 23:00:00     3018    7   Moderate
    """
    def __init__(self, cut_column_name, bin_column_name, quantile, **kwargs):
        """Initializes the operator

        Args:
            cut_column_name (str):
                Name of the column to perform the cut operation on
            bin_column_name (str):
                Name of the column representing the bins
            quantile (int or list-like of float):
                Number of quantiles. 10 for deciles, 4 for quartiles, etc.
                Alternately array of quantiles, e.g. [0, .25, .5, .75, 1.] for quartiles.
            **kwargs:
              key word arguments passed to pandas ``cut`` method

        """
        self.cut_column_name = cut_column_name
        self.bin_column_name = bin_column_name
        self.quantile = quantile
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
                result, bins = pd.qcut(data_frame[self.cut_column_name],
                                       self.quantile, **self.kwargs)

                data_frame[self.bin_column_name] = result
                processed.append((data_frame, bins))
            else:
                data_frame[self.bin_column_name] = pd.qcut(
                    data_frame[self.cut_column_name], self.quantile,
                    **self.kwargs)
                processed.append(data_frame)
        return processed
