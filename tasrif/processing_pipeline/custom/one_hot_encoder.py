"""
Operator to encode (transform) categorical features as a one-hot numeric array.
"""
import pandas as pd
from tasrif.processing_pipeline import ProcessingOperator



class OneHotEncoderOperator(ProcessingOperator):
    """

      Encodes categorical column features from existing features in the data frame.
      This operator works on a 2D data frames where the columns represent the features.
      A feature column with X different values will be replaced by X (or X-1) new column features.

      Examples
      --------

      >>> import pandas as pd
      >>> from tasrif.processing_pipeline.custom import OneHotEncoderOperator
      >>>
      >>> df = pd.DataFrame({'id': [1, 2, 3], 'colors': ['red', 'white', 'blue'],
      >>>             'cities': ['Doha', 'Vienna', 'Belo Horizonte'],
      >>>             'multiple': ["1,2", "1", "1,3"]
      >>> df
            id	colors	cities	        multiple
        0	1	red	    Doha	        1,2
        1	2	white	Vienna	        1
        2	3	blue	Belo Horizonte	1,3

      >>> OneHotEncoderOperator(feature_names=["colors"], drop_first=False).process(df)[0]
	        id	cities	        multiple	colors=red	colors=white
        0	1	Doha	        1,2	        1	        0
        1	2	Vienna	        1	        0	        1
        2	3	Belo Horizonte	1,3	        0	        0

      >>> OneHotEncoderOperator(feature_names=["colors"], drop_last_expansion=True).process(df)[0]
            id	cities	        multiple	colors=blue	colors=red	colors=white
        0	1	Doha	        1,2	        0	        1	        0
        1	2	Vienna	        1  	        0	        0	        1
        2	3	Belo Horizonte	1,3	        1	        0	        0

      >>> OneHotEncoderOperator(feature_names=["colors", "multiple"], drop_first=False).process(df)[0]
            id	cities	        colors=blue	colors=red	colors=white	multiple=1	multiple=2	multiple=3
        0	1	Doha	        0	        1	        0	            1	        1	        0
        1	2	Vienna	        0	        0	        1	            1	        0	        0
        2	3	Belo Horizonte	1	        0  	        0	            1	        0	        1

    """

    def __init__(self, feature_names: list, drop_first: bool = True, separator: str = ","):
        """Creates a new instance of OneHotEncoderOperator

        Args:
            feature_names (list):
                The list of categorical features that will be one hot encoded
            drop_first (bool):
                Transforming a category of X values into exactly X columns most of the times is redundant.
                If the values are NOT multiple choice, any of the new columns can be removed without
                resulting in loss of information, at least in a ML perspective.
                Default: False
            separator (str):
                That is the separator, if any, used when the values in a column are represented as multiple choice.
                Default: ','
        """
        super().__init__()
        self.feature_names = feature_names
        self.drop_first = drop_first
        self.separator = separator

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

            one_hot_acc = []
            for col in self.feature_names:
                # Convert values to str to be able to break lists of values
                one_hot = data_frame[col].astype(str).str.get_dummies(sep=self.separator)
                # Change col names with pattern col => col=value
                one_hot.columns = ["%s=%s" % (col, v) for v in one_hot.keys()]

                # Optionally removes the first col
                if self.drop_first:
                    del one_hot[one_hot.keys()[0]]

                one_hot_acc.append(one_hot)

            one_hot_acc = pd.concat(one_hot_acc, axis=1)

            # Remove original columns and append new cols to df
            data_frame = pd.concat((data_frame.drop(columns=self.feature_names), one_hot_acc), axis=1)

            # Save final result
            processed.append(data_frame)

        return processed
