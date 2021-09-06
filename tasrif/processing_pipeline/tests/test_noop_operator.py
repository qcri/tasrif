import pandas as pd
from tasrif.processing_pipeline import NoopOperator

def test_input_is_not_changed():
    input_dfs = [
        pd.DataFrame([1, 2, 3]),
        pd.DataFrame(["a", "b", "c"]),
        pd.DataFrame([None, None, None]),
    ]

    operator = NoopOperator()
    output_dfs = operator.process(*input_dfs)

    # Check that the output returned is the same as the input
    assert(len(output_dfs) == len(input_dfs))
    for i in range(len(output_dfs)):
        assert(output_dfs[i].equals(input_dfs[i]))
