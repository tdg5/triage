import logging


class SparseStateTableGenerator(object):
    """Take a dense state table and create a sparse state table"""
    def __init__(self, engine, experiment_hash):
        self.engine = engine
        self.experiment_hash = experiment_hash

    @property
    def table_name(self):
        return 'tmp_sparse_states_{}'.format(self.experiment_hash)

    def generate(self, dense_state_table, as_of_times):
        """
        input table:
        entity_id, state, start_date, end_date

        output table:
        entity_id, as_of_time, state_one, state_two
        """
        all_states = [
            row[0] for row in
            self.engine.execute('''
                select distinct(state) from {} order by state
            '''.format(dense_state_table))
        ]
        logging.info('Distinct states found: %s', all_states)
        state_columns = [
            'bool_or(state = \'{desired_state}\') as {desired_state}'.format(desired_state=state)
            for state in all_states
        ]
        query = '''
            create table {sparse_state_table} as (
            select d.entity_id, a.as_of_time::timestamp, {state_column_string}
                from {dense_state_table} d
                join (select unnest(ARRAY{as_of_times}) as as_of_time) a on (d.start_time <= a.as_of_time::timestamp and d.end_time > a.as_of_time::timestamp)
                group by d.entity_id, a.as_of_time
            )
        '''.format(
            sparse_state_table=self.table_name,
            dense_state_table=dense_state_table,
            as_of_times=[date.isoformat() for date in as_of_times],
            state_column_string=', '.join(state_columns)
        )
        self.engine.execute(query)
        self.engine.execute('create index on {} (entity_id, as_of_time)'.format(self.table_name))

    def clean_up(self):
        self.engine.execute('drop table {}'.format(self.table_name))
