"""
Operator that returns an iterator of json data.
"""
import pathlib
import json

from tasrif.processing_pipeline import ProcessingOperator
from tasrif.processing_pipeline import SequenceOperator

class IterateJsonOperator(ProcessingOperator):
    """
    Operator that returns an iterator of json data.

    Example
    -------
    >>> import json
    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.custom import IterateJsonOperator
    >>> df = pd.DataFrame({"name": ['Alfred', 'Roy'],
    ...                    "age": [43, 32],
    ...                    "file_details": ['details1.json', 'details2.json']})
    >>> details1 = [{'calories': [360, 540],
    ...              'time': "2015-04-25"}]
    >>> details2 = [{'calories': [420, 250],
    ...              'time': "2015-05-16"}]
    >>> # Save File 1 and File 2
    >>> json.dump(details1, open('details1.json', 'w+'))
    >>> json.dump(details2, open('details2.json', 'w+'))
    >>> operator = IterateJsonOperator(folder_path='./', field='file_details', pipeline=None)
    >>> generator = operator.process(df)[0]
    >>> # Iterates twice
    >>> for record, details in generator:
    ...     print('Subject information:')
    ...     print(record)
    ...     print('')
    ...     print('Subject details:')
    ...     print(details)
    ...     print('============================')
    Subject information:
    Pandas(Index=0, name='Alfred', age=43, file_details='details1.json')
    ...
    Subject details:
    [{'calories': [360, 540], 'time': '2015-04-25'}]
    \\============================
    Subject information:
    Pandas(Index=1, name='Roy', age=32, file_details='details2.json')
    ...
    Subject details:
    [{'calories': [420, 250], 'time': '2015-05-16'}]
    \\============================

    """
    def __init__(self, folder_path, field, pipeline: SequenceOperator):
        """Creates a new instance of IterateJsonOperator

        Args:
            folder_path (str):
                path to csv files
            field (str):
                column that contains the csv file names
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
                with open(self.folder_path.joinpath(json_file_name)) as json_file:
                    json_data = json.load(json_file)
                    if self.pipeline:
                        json_data = self.pipeline.process(json_data)[0]
                    yield (row, json_data)
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
