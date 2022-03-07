"""Module to export observers
"""

# VisualizeDaysObserver must be imported before DataPrepObserver to avoid
# issues with matplotlib.pyplot, which looks for IPython in sys.modules.
# DataprepObserver uses dataprep.eda imports IPython, and therefore causes
# matplotlib.pyplot to fail (if IPython shell is not being used)
from tasrif.processing_pipeline.observers.visualize_days_observer import (
    VisualizeDaysObserver,
)

# isort: split
from tasrif.processing_pipeline.observers.dataprep_observer import DataprepObserver
from tasrif.processing_pipeline.observers.functional_observer import FunctionalObserver
from tasrif.processing_pipeline.observers.groupby_logging_observer import (
    GroupbyLoggingOperator,
)
from tasrif.processing_pipeline.observers.logging_observer import LoggingObserver
