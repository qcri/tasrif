import pytest
from examples.my_heart_counts.quality_of_life_survey import pipeline

@pytest.fixture
def df(scope='module'):
    return pipeline.process()[0]

def test_data_is_correctly_loaded(df):
    columns = list(df.columns)
    columns.sort()

    assert(set(columns) ==  {'appVersion',
                             'createdOn','feel_worthwhile1','feel_worthwhile2','feel_worthwhile3',
                             'feel_worthwhile4','healthCode', 'phoneInfo', 'recordId',
                             'riskfactors1', 'riskfactors2', 'riskfactors3', 'riskfactors4',
                             'satisfiedwith_life', 'zip3'})

def test_healthCode_is_unique(df):
    assert(df['healthCode'].is_unique)

def test_df_has_no_NAs(df):
    assert(not df.isnull().values.any())
