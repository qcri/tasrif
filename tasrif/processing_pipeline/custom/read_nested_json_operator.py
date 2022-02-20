"""
Operator to aggregate column features based on a column
"""

import pathlib

import pandas as pd

from tasrif.processing_pipeline import ProcessingOperator, SequenceOperator


class ReadNestedJsonOperator(ProcessingOperator):
    """
    Operator that returns a Generator: one record per call.

    Example
    --------

    >>> import pandas as pd
    >>> import numpy as np
    >>>
    >>> from tasrif.processing_pipeline.custom import ReadNestedJsonOperator
    >>>
    >>> df = pd.DataFrame({"name": ['Alfred', 'Roy'],
    ...                    "age": [43, 32],
    ...                    "file_details": ['details1', 'details2']})
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
    >>> details1.to_json('details1.json', index=False)
    >>> details2.to_json('details2.json', index=False)
    >>>
    >>> operator = ReadNestedjsonOperator(folder_path='./', field='file_details', pipeline=None)
    >>> generator = operator.process(df)
    >>>
    >>> # Iterates twice
    >>> for record, details in generator:
    ...     print('Subject information:')
    ...     print(record)
    ...     print('')
    ...     print('Subject details:')
    ...     print(details)
    ...     print('============================')
    Subject information:
    name              Alfred
    age                   43
    file_details    details1
    Name: 0, dtype: object
    ...
    Subject details:
       calories        time
    0       360  2015-04-25
    1       540  2015-04-26
    ============================
    Subject information:
    name                 Roy
    age                   32
    file_details    details2
    Name: 1, dtype: object
    ...
    Subject details:
       calories        time
    0       420  2015-05-16
    1       250  2015-05-17
    ============================

    """

    def __init__(self, folder_path, field, pipeline: SequenceOperator = None):
        """Creates a new instance of ReadNestedjsonOperator

        Args:
            folder_path (str):
                path to json files
            field (str):
                column that contains the json file names
            pipeline (SequenceOperator):
                pipeline to apply on dataframe record before yielding it
        """
        super().__init__()
        self.folder_path = pathlib.Path(folder_path)
        self.field = field
        self.pipeline = pipeline

    def _create_json_generator(self, data_frame):
        for row in data_frame.itertuples():
            try:
                json_file_name = getattr(row, self.field)
                json_file = pd.read_json(self.folder_path.joinpath(json_file_name))
                if self.pipeline:
                    json_file = self.pipeline.process(json_file)[0]
                yield (row, json_file)
            except FileNotFoundError:
                yield (row, None)

    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            list: Tuple of series, and a Generator.
                    The series is the record information (one row of data_frame).
                    The generator returns a dataframe per next() call.
        """
        output = []
        for data_frame in data_frames:
            generator = self._create_json_generator(data_frame)
            output.append(generator)

        return output
