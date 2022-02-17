# +
import time

import pandas as pd

from tasrif.processing_pipeline import ComposeOperator, NoopOperator
from tasrif.processing_pipeline.processing_operator import ProcessingOperator

compose = ComposeOperator([NoopOperator(), 
                           NoopOperator(), 
                           NoopOperator(),
                           NoopOperator()],
                           num_processes=4)
compose.process()
# -


