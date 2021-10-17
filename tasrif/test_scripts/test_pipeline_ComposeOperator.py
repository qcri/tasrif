# +
import pandas as pd
import time
from tasrif.processing_pipeline import ComposeOperator
from tasrif.processing_pipeline.processing_operator import ProcessingOperator


# Sleeps for time and then returns empty dataframe
class SleepOperator(ProcessingOperator):

    def __init__(self, time):
        super().__init__()
        self.time = time
        
    def _process(self, *args):
        output = pd.DataFrame()
        time.sleep(self.time)
        return output

compose = ComposeOperator([SleepOperator(time=1), 
                           SleepOperator(time=1), 
                           SleepOperator(time=1),
                           SleepOperator(time=1)],
                           num_processes=4)
compose.process()
