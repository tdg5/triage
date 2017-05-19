from contextlib import contextmanager
import pandas
import tempfile
import yaml
import numpy
import random
from triage.db import Model
from sqlalchemy.orm import sessionmaker


@contextmanager
def fake_metta(matrix_dict, metadata):
    """Stores matrix and metadata in a metta-data-like form

    Args:
    matrix_dict (dict) of form { columns: values }.
        Expects an entity_id to be present which it will use as the index
    metadata (dict). Any metadata that should be set

    Yields:
        tuple of filenames for matrix and metadata
    """
    matrix = pandas.DataFrame.from_dict(matrix_dict).set_index('entity_id')
    with tempfile.NamedTemporaryFile() as matrix_file:
        with tempfile.NamedTemporaryFile('w') as metadata_file:
            hdf = pandas.HDFStore(matrix_file.name)
            hdf.put('title', matrix, data_columns=True)
            matrix_file.seek(0)

            yaml.dump(metadata, metadata_file)
            metadata_file.seek(0)
            yield (matrix_file.name, metadata_file.name)


def fake_labels(length):
    return numpy.array([random.choice([True, False]) for i in range(0, length)])


class MockTrainedModel(object):
    def predict_proba(self, dataset):
        return numpy.random.rand(len(dataset), len(dataset))


def fake_trained_model(project_path, model_storage_engine, db_engine):
    """Creates and stores a trivial trained model

    Args:
        project_path (string) a desired fs/s3 project path
        model_storage_engine (triage.storage.ModelStorageEngine)
        db_engine (sqlalchemy.engine)

    Returns:
        (int) model id for database retrieval
    """
    trained_model = MockTrainedModel()
    model_storage_engine.get_store('abcd').write(trained_model)
    session = sessionmaker(db_engine)()
    db_model = Model(model_hash='abcd')
    session.add(db_model)
    session.commit()
    return trained_model, db_model.model_id


def assert_index(engine, table, column):
    """Assert that a table has an index on a given column

    Does not care which position the column is in the index
    Modified from https://www.gab.lc/articles/index_on_id_with_postgresql

    Args:
        engine (sqlalchemy.engine) a database engine
        table (string) the name of a table
        column (string) the name of a column
    """
    query = """
        SELECT 1
        FROM pg_class t
             JOIN pg_index ix ON t.oid = ix.indrelid
             JOIN pg_class i ON i.oid = ix.indexrelid
             JOIN pg_attribute a ON a.attrelid = t.oid
        WHERE
             a.attnum = ANY(ix.indkey) AND
             t.relkind = 'r' AND
             t.relname = '{table_name}' AND
             a.attname = '{column_name}'
    """.format(
        table_name=table,
        column_name=column
    )
    num_results = len([row for row in engine.execute(query)])
    assert num_results >= 1


def create_dense_state_table(db_engine, table_name, data):
    db_engine.execute('''create table {} (
        entity_id int,
        state text,
        start_time timestamp,
        end_time timestamp
    )'''.format(table_name))

    for row in data:
        db_engine.execute(
            'insert into {} values (%s, %s, %s, %s)'.format(table_name),
            row
        )
