"""Module that defines the ProcessDataframeOperator class
"""

from tasrif.processing_pipeline.processing_operator import ProcessingOperator

class ProcessDataframeOperator(ProcessingOperator):
    """Operator to process on a single or multiple dataframes in the pipeline list.
    """

    def __init__(self, index, processing_operators):
        """Constructs a process dataframe operator

        Args:
            index: int or list[int]
                index of dataframe to process the processing_operators on.
                Dataframes not in the index will be passed.
            processing_operators : list[ProcessingOperator]
                Python list of processing operators

        Raises:
            ValueError: Occurs when one of the objects in the specified list is not a ProcessingOperator

        Examples
        --------

        >>> import pandas as pd
        >>> from tasrif.processing_pipeline ProcessDataframeOperator
        >>> from tasrif.processing_pipeline.custom import CreateFeatureOperator
        >>>
        >>> df0 = pd.DataFrame([[1, "2020-05-01 00:00:00", 1], [1, "2020-05-01 01:00:00", 1],
        ...                 [1, "2020-05-01 03:00:00", 2], [2, "2020-05-02 00:00:00", 1],
        ...                 [2, "2020-05-02 01:00:00", 1]],
        ...                 columns=['logId', 'timestamp', 'sleep_level'])
        >>>
        >>> df1 = pd.DataFrame([['tom', 10],
        ...                 ['Alfred', 15],
        ...                 ['Alfred', 18],
        ...                 ['juli', 14]],
        ...                 columns=['name', 'age'])
        >>>
        >>> df2 = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
        ...                 "toy": [None, 'Batmobile', 'Bullwhip'],
        ...                 "age": [38, 25, 23]})
        >>>
        >>> # Process df1, df2, but not df0
        >>> #
        >>> # Shortcut of doing
        >>> # ComposeOperator([
        >>> #     NoopOperator(),
        >>> #     feature_creator,
        >>> #     feature_creator,
        >>> # ])
        >>>
        >>> feature_creator = CreateFeatureOperator("name_age", lambda df: df["name"] + "_" + str(df["age"]))
        >>>
        >>> ProcessDataframeOperator(index=[1, 2], processing_operators=[feature_creator]).process(df0, df1, df2)
        [   logId            timestamp  sleep_level
         0      1  2020-05-01 00:00:00            1
         1      1  2020-05-01 01:00:00            1
         2      1  2020-05-01 03:00:00            2
         3      2  2020-05-02 00:00:00            1
         4      2  2020-05-02 01:00:00            1,
         [     name  age   name_age
          0     tom   10     tom_10
          1  Alfred   15  Alfred_15
          2  Alfred   18  Alfred_18
          3    juli   14    juli_14],
         [       name        toy  age     name_age
          0    Alfred       None   38    Alfred_38
          1    Batman  Batmobile   25    Batman_25
          2  Catwoman   Bullwhip   23  Catwoman_23]]

        """
        for operator in processing_operators:

            if not isinstance(operator, ProcessingOperator):
                raise ValueError("All operators in a pipeline must derive from ProcessingOperator!")

        self.index = index
        self.processing_operators = processing_operators


    def process(self, *data_frames):
        """Process the passed data using the processing configuration specified
        in the constructor

        Args:
            *data_frames (list of ProcessingOperator):
                Variable number of pandas dataframes to be applied on the indexed dataframe(s)

        Returns:
            output (list[pd.DataFrame]):
                processed dataframes

        Raises:
            IndexError: Occurs when self.index exceeds the length of data_frames

        """

        if isinstance(self.index, int):
            self.index = [self.index]

        if max(self.index) >= len(data_frames):
            raise IndexError("max index out of range: " + str(max(self.index)))

        data_frames = list(data_frames)
        for dataframe_index in self.index:
            for operator in self.processing_operators:
                data_frames[dataframe_index] = operator.process(data_frames[dataframe_index])

        return data_frames
