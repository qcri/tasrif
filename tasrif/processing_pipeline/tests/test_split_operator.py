from posixpath import split
from tasrif.processing_pipeline.processing_operator import ProcessingOperator
import pytest
from tasrif.processing_pipeline import SplitOperator

class NotProcessingOperator:
    pass

def test_error_is_raised_when_split_operators_are_not_ProcessingOperators(mocker):
    with pytest.raises(ValueError) as exc:
        SplitOperator([
            NotProcessingOperator(),
            NotProcessingOperator(),
            NotProcessingOperator()
        ])

def test_error_is_raised_when_bind_list_does_not_match_operators(mocker):
    split_operators = [
        mocker.Mock(spec=ProcessingOperator),
        mocker.Mock(spec=ProcessingOperator),
        mocker.Mock(spec=ProcessingOperator)
    ]
    bind_list = [0, 0]

    with pytest.raises(ValueError) as exc:
        SplitOperator(split_operators, bind_list=bind_list)

def test_args_are_split_correctly_when_no_bind_list_is_passed(mocker):
    split_operators = []
    mock_inputs = []
    for i in range(3):
        split_operator = mocker.Mock(spec=ProcessingOperator)
        split_operators.append(split_operator)

        mock_input = mocker.Mock()
        mock_inputs.append(mock_input)

    operator = SplitOperator(split_operators)
    operator.process(*mock_inputs)

    # Assert that the operators were called with the correct inputs.
    for i in range(len(split_operators)):
        correct_input = mock_inputs[i]
        split_operators[i].process.assert_called_once_with(correct_input)

def test_args_are_split_correctly_when_bind_list_is_passed(mocker):
    split_operators = []
    mock_inputs = []
    for i in range(3):
        split_operator = mocker.Mock(spec=ProcessingOperator)
        split_operators.append(split_operator)

        mock_input = mocker.Mock()
        mock_inputs.append(mock_input)

    bind_list = [2, 1, 0]

    operator = SplitOperator(split_operators, bind_list=bind_list)
    operator.process(*mock_inputs)

    # Assert that the operators were called with the correct inputs.
    for i in range(len(split_operators)):
        correct_input = mock_inputs[bind_list[i]]
        split_operators[i].process.assert_called_once_with(correct_input)
