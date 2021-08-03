# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
from tasrif.processing_pipeline import GeneratorProcessingOperator

def example_generator():
    for num in range(0, 10):
        yield num

def processing_function(generator):
    total = 0
    for num in generator:
        total += num
    return total

operator = GeneratorProcessingOperator(processing_function)

operator.process(example_generator())
