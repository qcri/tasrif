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

from tasrif.processing_pipeline.pandas import ConcatOperator

# Full
df1 = pd.DataFrame({'id': [1, 2, 3], 'cities': ['Rome', 'Barcelona', 'Stockholm']})
df2 = pd.DataFrame({'id': [4, 5, 6], 'cities': ['Doha', 'Vienna', 'Belo Horizonte']})

concat = ConcatOperator().process(df1, df2)
concat


# %%

from tasrif.processing_pipeline import SequenceOperator, ProcessingOperator
from tasrif.processing_pipeline.variables import enable_variables

# %%


class TrainingOperator(ProcessingOperator):

    @enable_variables()
    def __init__(self, model):
        super().__init__()

    def _process(self, *args):
        self.model.value = {'x': 1, 'y': 2}
        return args

class PredictionOperator(ProcessingOperator):

    @enable_variables()
    def __init__(self, model):
        super().__init__()

    def _process(self, *args):
        print(self.model.value)
        return args


s = SequenceOperator( model='{}', processing_operators=[
    TrainingOperator(model='{{model}}'),
    PredictionOperator(model='{{model}}'),
    SequenceOperator(modelb='{}', processing_operators=[
        TrainingOperator(model='{{modelb}}'),
        PredictionOperator(model='{{modelb}}')
    ])
])

# %%
s.process(df1, df2)

# %%
a = {'x': 1, 'y': 2}
b = {'y': 3, 'z': 4}
c = None

# %%
{**a, **b}

# %%
