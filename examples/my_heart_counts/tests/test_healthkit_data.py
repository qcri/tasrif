import pytest
import itertools
from examples.my_heart_counts.healthkit_data import pipeline

@pytest.fixture
def dataframe(scope='module'):
    dataframe = pipeline.process()[0]
    return dataframe

def test_data_is_correctly_loaded(dataframe):
    assert 'HKCategoryTypeIdentifierSleepAnalysis' in list(dataframe.columns)