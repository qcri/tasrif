# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import pandas as pd

from tasrif.processing_pipeline import PrintOperator


def test_input_is_not_changed():
    input_dfs = [
        pd.DataFrame([1, 2, 3]),
        pd.DataFrame(["a", "b", "c"]),
        pd.DataFrame([None, None, None]),
    ]

    operator = PrintOperator()
    output_dfs = operator.process(*input_dfs)

    # Check that the output returned is the same as the input
    assert len(output_dfs) == len(input_dfs)
    for i in range(len(output_dfs)):
        assert output_dfs[i].equals(input_dfs[i])


def test_input_is_printed(capsys):
    input_df = pd.DataFrame(
        [
            [1, "2020-05-01 00:00:00", 1],
            [1, "2020-05-01 01:00:00", 1],
            [1, "2020-05-01 03:00:00", 2],
            [2, "2020-05-02 00:00:00", 1],
            [2, "2020-05-02 01:00:00", 1],
        ],
        columns=["logId", "timestamp", "sleep_level"],
    )

    operator = PrintOperator()
    operator.process(input_df)

    printed_output = capsys.readouterr().out

    assert input_df.to_string() == printed_output.rstrip()
