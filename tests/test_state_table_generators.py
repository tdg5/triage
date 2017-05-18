from triage.state_table_generators import SparseStateTableGenerator
import testing.postgresql
from datetime import datetime
from sqlalchemy.engine import create_engine


def create_dense_state_table(db_engine, data):
    db_engine.execute('''create table states (
        entity_id int,
        state text,
        start_time timestamp,
        end_time timestamp
    )''')

    for row in data:
        db_engine.execute(
            'insert into states values (%s, %s, %s, %s)',
            row
        )


def test_sparse_state_table_generator():
    input_data = [
        (5, 'permitted', datetime(2016, 1, 1), datetime(2016, 6, 1)),
        (6, 'permitted', datetime(2016, 2, 5), datetime(2016, 5, 5)),
        (1, 'injail', datetime(2014, 7, 7), datetime(2014, 7, 15)),
        (1, 'injail', datetime(2016, 3, 7), datetime(2016, 4, 2)),
    ]

    with testing.postgresql.Postgresql() as postgresql:
        engine = create_engine(postgresql.url())
        create_dense_state_table(engine, input_data)

        table_generator = SparseStateTableGenerator(engine)
        as_of_times = [
            datetime(2016, 1, 1),
            datetime(2016, 2, 1),
            datetime(2016, 3, 1),
            datetime(2016, 4, 1),
            datetime(2016, 5, 1),
            datetime(2016, 6, 1),
        ]
        sparse_table_name = table_generator.generate('states', as_of_times)
        results = [row for row in engine.execute(
            'select entity_id, as_of_time, injail, permitted from {} order by entity_id, as_of_time'.format(
                sparse_table_name
            ))]
        expected_output = [
            # entity_id, as_of_time, injail, permitted
            (1, datetime(2016, 4, 1), True, False),
            (5, datetime(2016, 1, 1), False, True),
            (5, datetime(2016, 2, 1), False, True),
            (5, datetime(2016, 3, 1), False, True),
            (5, datetime(2016, 4, 1), False, True),
            (5, datetime(2016, 5, 1), False, True),
            (6, datetime(2016, 3, 1), False, True),
            (6, datetime(2016, 4, 1), False, True),
            (6, datetime(2016, 5, 1), False, True),
        ]
        import logging
        logging.warning(results)
        assert results == expected_output
