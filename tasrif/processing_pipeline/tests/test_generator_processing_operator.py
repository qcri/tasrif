from unittest.mock import call
from tasrif.processing_pipeline import GeneratorProcessingOperator

def test_processing_function_is_called(mocker):
    processing_function_stub = mocker.stub()
    generator_stub_1 = mocker.stub()
    generator_stub_2 = mocker.stub()

    operator = GeneratorProcessingOperator(processing_function_stub)
    operator.process(generator_stub_1, generator_stub_2)

    # Test that the processing_function has been called on the generators.
    processing_function_stub.assert_has_calls([
        call(generator_stub_1),
        call(generator_stub_2),
    ])

def test_correct_output_is_returned(mocker):
    # Set the processing_function to return predictable values
    processing_function_stub = mocker.stub()
    processing_function_stub.side_effect = [0, 1]

    generator_stub_1 = mocker.stub()
    generator_stub_2 = mocker.stub()

    operator = GeneratorProcessingOperator(processing_function_stub)
    output = operator.process(generator_stub_1, generator_stub_2)
    assert(output == [0, 1])
