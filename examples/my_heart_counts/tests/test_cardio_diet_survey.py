import pytest
from examples.my_heart_counts.cardio_diet_survey import pipeline

@pytest.fixture
def df(scope='module'):
    return pipeline.process()[0]

def test_data_is_correctly_loaded(df):
    columns = list(df.columns)
    columns.sort()

    assert(columns == [
        'appVersion', 'createdOn', 'fish', 'fruit', 'grains', 'healthCode',
        'phoneInfo', 'recordId', 'sodium', 'sugar_drinks', 'vegetable'
    ])
