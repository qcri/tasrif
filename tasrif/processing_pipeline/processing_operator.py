"""Module that defines the ProcessingOperator class
"""
class ProcessingOperator:
    """Interface specification of a processing operator
    The constructor of a concrete operator will provide options to configure the
    operation. The processing is invoked via the process method and the data to be
    processed is passed to the process method.
    """

    def process(self, *data_frames):
        """Process the passed data using the processing configuration specified
        in the constructor

        Parameters
        ----------
        data_frames:
          Variable number of pandas dataframes to be processed

        Returns
        -------
        data_frames
            Processed data frames
        """
