# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
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

class TrainingOperator(ProcessingOperator):

    def __init__(self):
        super().__init__()

    def _process(self, *args):
        self.kwargs['model'] = {'x': 1, 'y': 2}
        return args

class PredictionOperator(ProcessingOperator):

    def __init__(self):
        super().__init__()

    def _process(self, *args):
        print(self.model)
        if hasattr(self, 'modelb'):
            print(self.modelb)
        return args


s = SequenceOperator(processing_operators=[TrainingOperator(),
                                           PredictionOperator(),
                                           SequenceOperator(processing_operators=[
                                                                TrainingOperator(),
                                                                PredictionOperator()
                                                            ], modelb={'x': 2, 'y': 3})
                                          ]
                    )

# %%
s.process(df1, df2)

# %%
a = {'x': 1, 'y': 2}
b = {'y': 3, 'z': 4}
c = None

# %%
{**a, **b}
