import pytest

from examples.my_heart_counts.day_one_survey import pipeline


@pytest.fixture
def df(scope="module"):
    return pipeline.process()[0]


def test_data_is_correctly_loaded(df):
    columns = list(df.columns)
    columns.sort()

    assert columns == [
        "appVersion",
        "createdOn",
        "device",
        "healthCode",
        "labwork",
        "phoneInfo",
        "recordId",
    ]


def test_df_has_no_NAs(df):
    assert not df["device"].isnull().values.any()
    assert not df["labwork"].isnull().values.any()
