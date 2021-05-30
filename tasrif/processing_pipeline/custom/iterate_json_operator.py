"""
Operator that returns an iterator of json data.
"""
import pathlib
import json

from tasrif.processing_pipeline import ProcessingOperator
from tasrif.processing_pipeline import ProcessingPipeline

class IterateJsonOperator(ProcessingOperator):
    """
    Operator that returns an iterator of json data.
    """
    def __init__(self, folder_path, field, pipeline: ProcessingPipeline):
        self.folder_path = pathlib.Path(folder_path)
        self.field = field
        self.pipeline = pipeline

    def _create_json_generator(self, data_frame):
        for row in data_frame.itertuples():
            with open(self.folder_path.joinpath(getattr(row, self.field))) as json_file:
                json_data = json.load(json_file)
                if self.pipeline:
                    json_data = self.pipeline.process(json_data)[0]
                yield (row, json_data)

    def process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Returns
        -------
        Tuple of series, and a Generator.
        The series is the record information (one row of data_frame).
        The generator returns a dataframe per next() call.
        """
        output = []
        for data_frame in data_frames:
            generator = self._create_json_generator(data_frame)
            output.append(generator)

        return output
