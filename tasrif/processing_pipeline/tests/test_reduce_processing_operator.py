import pytest

from tasrif.processing_pipeline import ReduceProcessingOperator


class IncorrectReduceProcessingOperator(ReduceProcessingOperator):
    pass


class CorrectReduceProcessingOperator(ReduceProcessingOperator):
    # This will be replaced with a mock later.
    def _processing_function(self, element):
        return element


def test_raises_an_error_if_processing_function_is_not_defined(mocker):
    with pytest.raises(TypeError) as exc:
        IncorrectReduceProcessingOperator()


def test_processing_function_is_called_correctly_with_initial_value(mocker):
    # Set initial to a mock value
    operator = CorrectReduceProcessingOperator()
    operator.initial = mocker.Mock()

    # Substitute the _processing_function with a stub that returns values.
    return_values = [mocker.Mock(), mocker.Mock(), mocker.Mock()]
    operator._processing_function = mocker.stub()
    operator._processing_function.side_effect = return_values

    args = [mocker.Mock(), mocker.Mock(), mocker.Mock()]
    output = operator.process(*args)

    # Assert that the processing function was called with the correct args and
    # return value.
    operator._processing_function.assert_has_calls(
        [
            mocker.call(args[0], operator.initial),
            mocker.call(args[1], return_values[0]),
            mocker.call(args[2], return_values[1]),
        ]
    )

    # Assert that the output is the last return value of the
    # _processing_function.
    assert output == return_values[2]


def test_processing_function_is_called_correctly_without_initial_value(mocker):
    operator = CorrectReduceProcessingOperator()

    # Substitute the _processing_function with a stub that returns values.
    return_values = [mocker.Mock(), mocker.Mock()]
    operator._processing_function = mocker.stub()
    operator._processing_function.side_effect = return_values

    args = [mocker.Mock(), mocker.Mock(), mocker.Mock()]
    output = operator.process(*args)

    # Assert that the processing function was called with the correct args and
    # return value.
    operator._processing_function.assert_has_calls(
        [mocker.call(args[1], args[0]), mocker.call(args[2], return_values[0])]
    )

    # Assert that the output is the last return value of the
    # _processing_function.
    assert output == return_values[1]
