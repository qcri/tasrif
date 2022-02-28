import pytest

from examples.my_heart_counts.activity_sleep_survey import pipeline


@pytest.fixture
def df(scope="module"):
    return pipeline.process()[0]


def test_data_is_correctly_loaded(df):
    columns = list(df.columns)
    columns.sort()

    assert columns == [
        "appVersion",
        "atwork",
        "createdOn",
        "healthCode",
        "moderate_act",
        "phoneInfo",
        "phys_activity",
        "recordId",
        "sleep_diagnosis1",
        "sleep_time",
        "sleep_time1",
        "vigorous_act",
        "work",
    ]


def test_healthCode_is_unique(df):
    assert df["healthCode"].is_unique


def test_df_has_no_NAs(df):
    assert not df.isnull().values.any()
