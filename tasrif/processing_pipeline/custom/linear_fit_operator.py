"""
Operator to fit features to target columns using sklearn's linear regression
"""
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from tasrif.processing_pipeline import ProcessingOperator


class LinearFitOperator(ProcessingOperator):
    """

    Operator to fit features to target column using linear regression

    Examples
    --------

    >>> import pandas as pd
    >>> from tasrif.processing_pipeline.custom import LinearFitOperator
    >>> df = pd.DataFrame([
    ...     [1, "2020-05-01 00:00:00", 10, 'poor'],
    ...     [1, "2020-05-01 01:00:00", 15, 'poor'],
    ...     [1, "2020-05-01 03:00:00", 23, 'good'],
    ...     [2, "2020-05-02 00:00:00", 17, 'good'],
    ...     [2, "2020-05-02 01:00:00", 11, 'poor']],
    ...     columns=['logId', 'timestamp', 'sleep_level', 'sleep_quality'])
    >>>
    >>> op = LinearFitOperator(feature_names='sleep_level',
    ...                        target='sleep_quality',
    ...                        target_type='categorical')
    >>> print(op.process(df))
    [(array(['poor', 'poor', 'good', 'good', 'poor'], dtype=object), 1.0, array([12.71063824]))]

    >>> df = pd.DataFrame([
    ...     [15, 10, 'poor'],
    ...     [13, 15, 'poor'],
    ...     [11, 23, 'good'],
    ...     [25, 17, 'good'],
    ...     [20, 11, 'poor']],
    ...     columns=['feature1', 'feature2', 'target'])
    >>>
    >>> op = LinearFitOperator(feature_names='all',
    ...                        target='target',
    ...                        target_type='categorical')
    >>> op.process(df)
    [(array(['poor', 'poor', 'good', 'good', 'poor'], dtype=object),
      1.0,
      array([17.78134321]))]

    """
    def __init__(self,
                 feature_names,
                 target,
                 target_type='continuous',
                 **model_kwargs):
        """
        Creates a new instance of LinearFitOperator

        Args:
            feature_names (list, str):
                feature_names in the given dataframe to fit. if 'all', then select all numerical features except target
            target (str):
                dependant target feature in the dataframe
            target_type (str):
                - If target_type is ``continuous``, LinearRegression will be used
                - If target_type is ``categorical``, LogisticRegression will be used
                - else, LogisticRegression will be used
            **model_kwargs:
                key word arguments passed to sklearn LinearRegression or LogisticRegression

        """
        super().__init__()
        self.feature_names = feature_names
        self.target = target
        self.target_type = target_type
        self.model_kwargs = model_kwargs

        if self.target_type == 'continuous':
            self.model = LinearRegression(**self.model_kwargs)
        else:
            self.model = LogisticRegression(**self.model_kwargs)


    def _process(self, *data_frames):
        """Processes the passed data frame as per the configuration define in the constructor.

        Args:
            *data_frames (list of pd.DataFrame):
              Variable number of pandas dataframes to be processed

        Returns:
            a tuple of
                - Correlation coefficient R2
                - Model coefficients (slope)
                - Model bias (intercept)

        """

        processed = []
        for data_frame in data_frames:

            if self.feature_names == 'all':
                self.feature_names = data_frame.select_dtypes(include=np.number).columns.tolist()
                if self.target in self.feature_names:
                    self.feature_names.remove(self.target)

            if isinstance(self.feature_names, str):
                self.feature_names = [self.feature_names]

            model_input = data_frame[self.feature_names].values.reshape(-1, len(self.feature_names))
            model_target = data_frame[self.target].values.flatten()

            self.model.fit(model_input, model_target)


            processed.append((self.model.score(model_input, model_target),
                              self.model.coef_,
                              self.model.intercept_))

        return processed
