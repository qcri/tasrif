"""
Operator to read multiple csvs in a folder
"""

import pathlib
import glob
import os
import pandas as pd

from tasrif.processing_pipeline import ProcessingOperator
from tasrif.processing_pipeline import SequenceOperator

class ReadCsvFolderOperator(ProcessingOperator):
    """
    Operator that returns a Generator: one csv file per call.

    Example
    --------

    >>> import pandas as pd
    >>> import numpy as np
    >>>
    >>> from tasrif.processing_pipeline.custom import ReadCsvFolderOperator
    >>> from tasrif.processing_pipeline.pandas import ConcatOperator
    >>> from tasrif.processing_pipeline import SequenceOperator
    >>>
    >>>
    >>> details1 = pd.DataFrame({'calories': [360, 540],
    ...                          'time': [pd.Timestamp("2015-04-25"), pd.Timestamp("2015-04-26")]
    ...                         })
    >>>
    >>> details2 = pd.DataFrame({'calories': [420, 250],
    ...                          'time': [pd.Timestamp("2015-05-16"), pd.Timestamp("2015-05-17")]
    ...                         })
    >>>
    >>>
    >>> # Save File 1 and File 2
    >>> details1.to_csv('./details1.csv', index=False)
    >>> details2.to_csv('./details2.csv', index=False)
    >>>
    >>> pipeline = SequenceOperator([
    ...     ReadCsvFolderOperator(name_pattern='./*.csv', pipeline=None),
    ...     ConcatOperator()
    ... ])
    >>>
    >>> df = pipeline.process()[0]
    >>> df

    """

    def __init__(self,
                 pipeline: SequenceOperator = None,
                 name_pattern='*.csv',
                 filename_column_name='filename',
                 concatenate=True,
                 **read_csv_kwargs):
        """Creates a new instance of ReadCsvFolderOperator

        Args:
            pipeline (SequenceOperator):
                pipeline to apply on dataframe csv file before yielding it
            name_pattern (str):
                regex pattern of the csv files that the user wishes to read
            filename_column_name (str):
                column to be created for the csv file representing the filename
            concatenate (bool):
                whether to concatenate the files to a single dataframe or not
            **read_csv_kwargs:
                keyword arguments passed to Pandas read_csv method
        """
        super().__init__()
        self.pipeline = pipeline
        self.read_csv_kwargs = read_csv_kwargs
        self.name_pattern = pathlib.Path(name_pattern)
        self.filename_column_name = filename_column_name
        self.concatenate = concatenate

    def _read_csvs_in_folder(self):
        files = glob.glob(str(self.name_pattern))
        for file in files:
            csv_file = pd.read_csv(file, **self.read_csv_kwargs)
            csv_file[self.filename_column_name] = os.path.basename(file)
            if self.pipeline:
                csv_file = self.pipeline.process(csv_file)[0]
            yield csv_file

    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            output (Generator):
              each yield returns a dataframe representing a csv file,
              with an added column filename

        """

        generator = self._read_csvs_in_folder()

        if self.concatenate:
            output = list(generator)
            output = pd.concat(output)
            output = [output]
            return output

        return generator
