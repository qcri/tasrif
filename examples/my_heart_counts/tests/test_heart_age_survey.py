import pytest
from examples.my_heart_counts.heart_age_survey import pipeline

@pytest.fixture
def df(scope='module'):
    return pipeline.process()[0]

def test_data_is_correctly_loaded(df):
    columns = list(df.columns)
    columns.sort()

    assert(columns == [
        'appVersion', 'bloodPressureInstruction', 'bloodPressureInstruction_unit', 'createdOn',
        'healthCode', 'heartAgeDataAge', 'heartAgeDataBloodGlucose', 'heartAgeDataBloodGlucose_unit',
        'heartAgeDataDiabetes', 'heartAgeDataEthnicity', 'heartAgeDataGender', 'heartAgeDataHdl',
        'heartAgeDataHdl_unit', 'heartAgeDataHypertension', 'heartAgeDataLdl', 'heartAgeDataLdl_unit',
        'heartAgeDataSystolicBloodPressure', 'heartAgeDataSystolicBloodPressure_unit',
        'heartAgeDataTotalCholesterol', 'heartAgeDataTotalCholesterol_unit', 'phoneInfo',
        'recordId', 'smokingHistory'
    ])

def test_healthCode_is_unique(df):
    assert(df['healthCode'].is_unique)

def test_df_has_no_NAs(df):
    assert(not df.isnull().values.any())
