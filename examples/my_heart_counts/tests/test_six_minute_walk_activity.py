import itertools

import pytest

from examples.my_heart_counts.six_minute_walk_activity import pipeline


@pytest.fixture
def dataframe(scope="module"):
    dataframe = pipeline.process()[0]
    return dataframe


def test_data_is_correctly_loaded(dataframe):
    assert list(dataframe.columns) == ["recordId", "numberOfSteps_max"]
