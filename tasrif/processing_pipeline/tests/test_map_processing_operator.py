import pytest
from tasrif.processing_pipeline import MapProcessingOperator

class IncorrectMapProcessingOperator(MapProcessingOperator):
    pass

class CorrectMapProcessingOperator(MapProcessingOperator):
    # This will be replaced with a mock later.
    def processing_function(self, element):
        return element

def test_raises_an_error_if_processing_function_is_not_defined(mocker):
    with pytest.raises(TypeError) as exc:
        IncorrectMapProcessingOperator()

def test_processing_function_is_called_correctly(mocker):
    operator = CorrectMapProcessingOperator()

    # Substitute the processing_function with a stub that returns values.
    return_values = [mocker.Mock(), mocker.Mock()]
    operator.processing_function = mocker.stub()
    operator.processing_function.side_effect = return_values

    args = [mocker.Mock(), mocker.Mock()]
    output = operator.process(*args)

    # Assert that the processing function was called with each of the individual
    # args.
    operator.processing_function.assert_has_calls([
        mocker.call(args[0]),
        mocker.call(args[1])
    ])

    # Assert that the output is returned in correct order.
    assert(output == return_values)






