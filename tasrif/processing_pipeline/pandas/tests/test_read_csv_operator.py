from tasrif.processing_pipeline.pandas import ReadCsvOperator


def test_read_csv_is_called(mocker):
    pandas_mock = mocker.patch('tasrif.processing_pipeline.pandas.read_csv_operator.pd')

    args = ['arg1', 'arg2']
    kwargs = {'kwarg1': None, 'kwarg2': None}

    ReadCsvOperator(*args, **kwargs).process()

    pandas_mock.read_csv.assert_called_once_with(*args, **kwargs)
