# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
from tasrif.processing_pipeline import ProcessingOperator
from tasrif.processing_pipeline.iterate_operator import IterateOperator

class StreamOperator(ProcessingOperator):
    def process(self, stream):
        output = []
        for x,y in stream:
            output.append(x + y)
        return output

def generatorA():
    for i in range(0, 10):
        yield (i, i)

def generatorB():
    for i in range(0, 10):
        yield (i, i + 1)

# generatorA and generatorB represent independent dataframes
op = IterateOperator(StreamOperator())
op.process(generatorA(), generatorB())
