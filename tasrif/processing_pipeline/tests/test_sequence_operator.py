import pytest
from tasrif.processing_pipeline import ProcessingOperator, SequenceOperator, sequence_operator

class NotProcessingOperator:
    pass

def test_raises_an_error_if_inputs_are_not_ProcessingOperators(mocker):
    with pytest.raises(ValueError) as exc:
        SequenceOperator([
            NotProcessingOperator(),
            NotProcessingOperator(),
            NotProcessingOperator()
        ])

def test_operators_are_called_correctly(mocker):
    sub_operators = []
    sub_operator_return_values = []
    # Create 3 "mock" operators for use in the SequenceOperator.
    # Set their .process return values to be mock values.
    for i in range(3):
        sub_operator = mocker.Mock(spec=ProcessingOperator)
        sub_operator.process.return_value = [mocker.Mock()]

        sub_operators.append(sub_operator)
        sub_operator_return_values.append(sub_operator.process.return_value)

    sequence_operator = SequenceOperator(sub_operators)

    args = [mocker.Mock(), mocker.Mock()]
    output = sequence_operator.process(*args)

    # Assert that operators were called sequentially and return values are
    # passed down through the pipeline.
    sub_operators[0].process.assert_called_once_with(*args)
    sub_operators[1].process.assert_called_once_with(*sub_operator_return_values[0])
    sub_operators[2].process.assert_called_once_with(*sub_operator_return_values[1])

    # Assert that the output is the return value of the last operator in the
    # pipeline.
    assert(output == sub_operator_return_values[2])
