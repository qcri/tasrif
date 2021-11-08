# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2
import pandas as pd
from tasrif.processing_pipeline import SequenceOperator, ProcessingOperator, ScopedProcessingOperator, Variable


# %%
df1 = pd.DataFrame({'id': [1, 2, 3], 'cities': ['Rome', 'Barcelona', 'Stockholm']})
df2 = pd.DataFrame({'id': [4, 5, 6], 'cities': ['Doha', 'Vienna', 'Belo Horizonte']})

class TrainingOperator(ProcessingOperator):

    def __init__(self, model, x=1, y=2):
        super().__init__()
        self.model = model
        self.x = x
        self.y = y

    def _process(self, *args):
        self.model.value = {'x': self.x, 'y': self.y}
        return args

class PredictionOperator(ProcessingOperator):

    def __init__(self, model):
        super().__init__()
        self.model = model

    def _process(self, *args):
        print(self.model.value)
        return args


modela = Variable(None)
s = SequenceOperator(processing_operators=[
    TrainingOperator(model=modela),
    PredictionOperator(model=modela),
    ScopedProcessingOperator(lambda modelb=Variable():
        SequenceOperator(processing_operators=[
            TrainingOperator(model=modelb),
            PredictionOperator(model=modelb),
            ScopedProcessingOperator(lambda :
                SequenceOperator(processing_operators=[
                    TrainingOperator(model=modelb),
                    PredictionOperator(model=modelb)]))
        ])            
    )
])

# %%
s.process(df1, df2)

# %%
