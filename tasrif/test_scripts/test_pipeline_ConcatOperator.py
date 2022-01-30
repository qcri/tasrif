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
print(concat[0])

# Test generator
gen = (pd.DataFrame([1]) for i in range(3))
print(ConcatOperator().process(gen)[0])


