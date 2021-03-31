"""
Operator to aggregate column features based on a column
"""

import pathlib
import pandas as pd

from tasrif.processing_pipeline import ProcessingOperator
from tasrif.processing_pipeline import ProcessingPipeline

class IterateCsvOperator(ProcessingOperator):
    """
    Operator that returns a Generator: one record per call.

    Returns
    -------
    int
    dataframe that represents one record


    Example
    --------

    >>> import pandas as pd
	>>> import numpy as np
	>>>
	>>> from tasrif.processing_pipeline.custom import IterateCsvOperator
	>>>
	>>> df = pd.DataFrame({"name": ['Alfred', 'Roy'],
	>>>                    "age": [43, 32],
	>>>                    "file_details": ['details1', 'details2']})
	>>>
	>>> details1 = pd.DataFrame({'calories': [360, 540],
	>>>                          'time': [pd.Timestamp("2015-04-25"), pd.Timestamp("2015-04-26")]
	>>>                         })
	>>>
	>>> details2 = pd.DataFrame({'calories': [420, 250],
	>>>                          'time': [pd.Timestamp("2015-05-16"), pd.Timestamp("2015-05-17")]
	>>>                         })
	>>>
	>>>
	>>> # Save File 1 and File 2
	>>> details1.to_csv('details1.csv', index=False)
	>>> details2.to_csv('details2.csv', index=False)
	>>>
	>>> operator = IterateCsvOperator(folder_path='./', field='file_details', pipeline=None)
	>>> generator = operator.process(df)
	>>>
	>>> # Iterates twice
	>>> for record, details in generator:
	>>>     print('Subject information:')
	>>>     print(record)
	>>>     print('')
	>>>     print('Subject details:')
	>>>     print(details)
	>>>     print('============================')


	Subject information:
	name              Alfred
	age                   43
	file_details    details1
	Name: 0, dtype: object

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

	Subject details:
	   calories        time
	0       420  2015-05-16
	1       250  2015-05-17
	============================

	"""

    def __init__(self, folder_path, field, pipeline: ProcessingPipeline = None):
        """Creates a new instance of IterateCsvOperator

        Parameters
        ----------
        folder_path: str
        	path to csv files
        field: str
        	column that contains the csv file names
        pipeline : ProcessingPipeline
        	pipeline to apply on dataframe record before yielding it
        """
        self.folder_path = pathlib.Path(folder_path)
        self.field = field
        self.pipeline = pipeline


    def process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Returns
        -------
        Tuple of series, and a Generator.
        The series is the record information (one row of data_frame).
        The generator returns a dataframe per next() call.
        """

        for data_frame in data_frames:
            for row in data_frame.itertuples():
                csv_file = pd.read_csv(self.folder_path.joinpath(getattr(row, self.field)))
                if self.pipeline:
                    csv_file = self.pipeline.process(csv_file)[0]

                yield (row, csv_file)