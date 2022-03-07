import pytest

from examples.my_heart_counts.parq_survey import pipeline


@pytest.fixture
def df(scope="module"):
    return pipeline.process()[0]


def test_data_is_correctly_loaded(df):
    columns = list(df.columns)
    columns.sort()

    assert columns == [
        "appVersion",
        "chestPain",
        "chestPainInLastMonth",
        "createdOn",
        "dizziness",
        "healthCode",
        "heartCondition",
        "jointProblem",
        "phoneInfo",
        "physicallyCapable",
        "prescriptionDrugs",
        "recordId",
    ]


def test_healthCode_is_unique(df):
    assert df["healthCode"].is_unique


def test_df_has_no_NAs(df):
    assert not df.isnull().values.any()
