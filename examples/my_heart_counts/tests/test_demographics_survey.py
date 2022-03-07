import pytest

from examples.my_heart_counts.demographics_survey import pipeline


@pytest.fixture
def df(scope="module"):
    return pipeline.process()[0]


def test_data_is_correctly_loaded(df):
    columns = list(df.columns)
    columns.sort()

    assert columns == [
        "appVersion",
        "createdOn",
        "healthCode",
        "patientBiologicalSex",
        "patientCurrentAge",
        "patientGoSleepTime",
        "patientHeightInches",
        "patientWakeUpTime",
        "patientWeightPounds",
        "phoneInfo",
        "recordId",
    ]


def test_healthCode_is_unique(df):
    assert df["healthCode"].is_unique


def test_df_has_no_NAs(df):
    assert not df.isnull().values.any()
