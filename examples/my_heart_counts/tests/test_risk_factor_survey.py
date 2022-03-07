import pytest

from examples.my_heart_counts.risk_factor_survey import pipeline


@pytest.fixture
def df(scope="module"):
    return pipeline.process()[0]


def test_data_is_correctly_loaded(df):
    columns = list(df.columns)
    columns.sort()

    assert columns == [
        "appVersion",
        "createdOn",
        "family_history",
        "healthCode",
        "heart_disease",
        "medications_to_treat",
        "phoneInfo",
        "recordId",
        "vascular",
    ]
