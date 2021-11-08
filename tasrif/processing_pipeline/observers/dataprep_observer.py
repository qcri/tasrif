"""Module that defines the Dataprep class
"""
from dataprep.eda import plot, plot_correlation, plot_missing, create_report
from tasrif.processing_pipeline.observers.functional_observer import FunctionalObserver

class DataprepObserver(FunctionalObserver):
    """DataprepObserver class to create a report for a dataframe
    """

    def __init__(self, method='full', **kwargs):
        """
        DataprepObserver constructor

        Args:
            method (String):
                Analysis method for the dataframe
                Options: "distribution", "correlation", "diff", "missing", "full"
            **kwargs:
              key word arguments passed to method

        """
        self._methods = []
        self.kwargs = kwargs
        if method:
            self._methods = method.split(',')

    def _observe(self, operator, *data_frames):
        """
        Observe the passed data using the processing configuration specified
        in the constructor

        Args:
            operator (ProcessingOperator):
                Processing operator which is observed
            *data_frames (list of pd.DataFrame):
                Variable number of pandas dataframes to be observed
        """
        for data_frame in data_frames:
            for analysis in self._methods:
                if analysis == 'distribution':
                    plot(data_frame[0], **self.kwargs).show()

                if analysis == 'correlation':
                    plot_correlation(data_frame[0], **self.kwargs).show()

                if analysis == 'missing':
                    plot_missing(data_frame[0], **self.kwargs).show()

                if analysis == 'full':
                    create_report(data_frame[0], **self.kwargs).show()
