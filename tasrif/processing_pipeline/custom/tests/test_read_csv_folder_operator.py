import pandas as pd
import types

from tasrif.processing_pipeline.custom import ReadCsvFolderOperator

def test_type_of_return_when_read_csv_folder_is_called(mocker):
    args = ['arg1', 'arg2']
    kwargs = {'kwarg1': None, 'kwarg2': None}

    generator = ReadCsvFolderOperator(*args, **kwargs).process()
    assert isinstance(generator, types.GeneratorType)


def test_read_csv_folder_is_called_n_number_of_times(mocker):
    # Mock glob
    glob_mock = mocker.patch('tasrif.processing_pipeline.custom.read_csv_folder_operator.glob')
    glob_mock.glob.return_value = [
                'path/to/folder/1.csv',
                'path/to/folder/2.csv',
                'path/to/folder/3.csv',
    ]

    pandas_mock = mocker.patch('tasrif.processing_pipeline.custom.read_csv_folder_operator.pd')
    pandas_mock.read_csv.return_value = pd.DataFrame() 

    generator = ReadCsvFolderOperator(name_pattern='path/to/folder/*.csv').process()
    list(generator)

    expected_call_count = 3
    actual_call_count = pandas_mock.read_csv.call_count
    assert expected_call_count == actual_call_count
