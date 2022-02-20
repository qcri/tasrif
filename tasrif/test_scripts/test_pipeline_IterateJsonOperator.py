# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: env
#     language: python
#     name: env
# ---

# %%
import json

import pandas as pd

from tasrif.processing_pipeline.custom import IterateJsonOperator

df = pd.DataFrame(
    {
        "name": ["Alfred", "Roy"],
        "age": [43, 32],
        "file_details": ["details1.json", "details2.json"],
    }
)

details1 = [{"calories": [360, 540], "time": "2015-04-25"}]

details2 = [{"calories": [420, 250], "time": "2015-05-16"}]

# Save File 1 and File 2
json.dump(details1, open("details1.json", "w+"))
json.dump(details2, open("details2.json", "w+"))

operator = IterateJsonOperator(folder_path="./", field="file_details", pipeline=None)
generator = operator.process(df)[0]

# Iterates twice
for record, details in generator:
    print("Subject information:")
    print(record)
    print("")
    print("Subject details:")
    print(details)
    print("============================")
