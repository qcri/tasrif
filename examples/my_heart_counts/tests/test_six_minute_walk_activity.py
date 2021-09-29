import pytest
import itertools
from examples.my_heart_counts.six_minute_walk_activity import pipeline

# To save time, test only the first few elements of the generator.
NUM_ELEMENTS_TO_TEST = 1000

@pytest.fixture
def generator(scope='module'):
    generator = pipeline.process()[0]
    return list(itertools.islice(generator, NUM_ELEMENTS_TO_TEST))

def test_data_is_correctly_loaded(generator):
    for record, csv in generator:
        # IterateJsonOperator returns the record as a NamedTuple with some extra
        # fields, just check for a subset of them.
        record_columns = list(record._fields)
        for column in ['recordId', 'healthCode', 'createdOn',
                       'appVersion', 'phoneInfo', 'file_name']:
            assert(column in record_columns)

        if csv is None:
            continue

        csv_columns = list(csv.columns)
        csv_columns.sort()
        assert(csv_columns == [
            'distance', 'endDate', 'floorsAscended',
            'floorsDescended', 'numberOfSteps', 'startDate'
        ])