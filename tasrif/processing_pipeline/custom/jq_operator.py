"""
Operator to resample a timeseries based dataframe
"""
import pyjq

from tasrif.processing_pipeline import ProcessingOperator

class JqOperator(ProcessingOperator):
    """

    Applies a jq processing expression to the input JSON data.

    Examples
    --------
    >>>
    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.custom import JqOperator
    >>> df = [
    >>> {
    >>>     "date": "2020-01-01",
    >>>     "sleep": [
    >>>     {
    >>>         "sleep_data": [
    >>>         {
    >>>             "level": "rem",
    >>>             "minutes": 180
    >>>         },
    >>>         {
    >>>             "level": "deep",
    >>>             "minutes": 80
    >>>         },
    >>>         {
    >>>             "level": "light",
    >>>             "minutes": 300
    >>>         }
    >>>         ]
    >>>     }
    >>>     ]
    >>> },
    >>> {
    >>>     "date": "2020-01-02",
    >>>     "sleep": [
    >>>     {
    >>>         "sleep_data": [
    >>>         {
    >>>             "level": "rem",
    >>>             "minutes": 280
    >>>         },
    >>>         {
    >>>             "level": "deep",
    >>>             "minutes": 60
    >>>         },
    >>>         {
    >>>             "level": "light",
    >>>             "minutes": 200
    >>>         }
    >>>         ]
    >>>     }
    >>>     ]
    >>> }
    >>> ]
    >>>
    >>> op = JqOperator("map({date, sleep: .sleep[].sleep_data})")
    >>> op.process(df)
    [[{'date': '2020-01-01',
    \'sleep': [{'level': 'rem', 'minutes': 180},
    {'level': 'deep', 'minutes': 80},
    {'level': 'light', 'minutes': 300}]},
    {'date': '2020-01-02',
    \'sleep': [{'level': 'rem', 'minutes': 280},
    {'level': 'deep', 'minutes': 60},
    {'level': 'light', 'minutes': 200}]}]]

    """

    def __init__(self, expression):
        """Creates a new instance of ResampleOperator

        Args:
            expression (str):
                JQ expression to process the incoming JSON datasets (see https://stedolan.github.io/jq/manual/) for
                more details
        """
        super().__init__()
        self.expression = expression


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
            data_frame = pyjq.first(self.expression, data_frame)
            processed.append(data_frame)

        return processed
