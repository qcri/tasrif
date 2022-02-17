import random

import pytest

from tasrif.processing_pipeline import ComposeOperator, ProcessingOperator


class NotProcessingOperator:
    pass

def test_raises_an_error_if_inputs_are_not_ProcessingOperators(mocker):
    with pytest.raises(ValueError) as exc:
        ComposeOperator([
            NotProcessingOperator(),
            NotProcessingOperator(),
            NotProcessingOperator()
        ])

def test_operators_are_called_correctly(mocker):
    sub_operators = []
    # Create 3 "mock" operators for use in the ComposeOperator.
    # Set their .process return values to be mock values.
    for i in range(3):
        sub_operator = mocker.Mock(spec=ProcessingOperator)
        sub_operator.process.return_value = mocker.Mock()
        sub_operators.append(sub_operator)

    compose_operator = ComposeOperator(sub_operators)

    args = [mocker.Mock(), mocker.Mock()]
    output = compose_operator.process(*args)

    for i, sub_operator in enumerate(sub_operators):
        # Assert all operators had .process called with the same args
        sub_operator.process.assert_called_once_with(*args)

        # Assert that their return values were captured
        assert(output[i] == sub_operator.process.return_value)
