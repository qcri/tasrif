import pytest
from examples.my_heart_counts.quality_of_life_survey import pipeline

@pytest.fixture
def df(scope='module'):
    return pipeline.process()[0]

def test_data_is_correctly_loaded(df):
    columns = list(df.columns)
    columns.sort()

    assert(columns == [
        'activity1_intensity', 'activity1_option', 'activity1_time', 'activity1_type',
        'activity2_intensity', 'activity2_option', 'activity2_time', 'activity2_type',
        'appVersion', 'createdOn', 'healthCode', 'phoneInfo',
        'phone_on_user', 'recordId', 'sleep_time'
    ])

def test_healthCode_is_unique(df):
    assert(df['healthCode'].is_unique)

def test_df_has_no_NAs(df):
    assert(not df.isnull().values.any())
