import io
import csv
from datetime import date, timedelta
import unittest
import os
import random
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tempfile import TemporaryDirectory
import testing.postgresql

from triage.db import ensure_db, Evaluation
from triage.storage import FSModelStorageEngine

from triage.pipelines import SerialPipeline, LocalParallelPipeline

BASE_RATE = 0.2

# Synthesize a dataset that will allow a 1-deep tree to learn
# that > 5 complaints in a year means violation

CUTOFF = 5


def collect_entity_year_data(entity_id, year, n_complaints):
    first_day_of_year = date(year, 1, 1)
    complaint_rows = []
    for _ in range(0, n_complaints):
        day_of_complaint = first_day_of_year + \
            timedelta(days=random.randint(1, 364))
        complaint_rows.append((entity_id, day_of_complaint, 1))

    outcome = 1 if n_complaints > CUTOFF else 0
    outcome_row = (entity_id, date(year, 12, 31), outcome)
    return complaint_rows, outcome_row


def populate_source_data(db_engine):
    db_engine.execute('''create table cat_complaints (
        entity_id int,
        as_of_date date,
        cat_was_sighted int
        )''')

    db_engine.execute('''create table inspections (
        entity_id int,
        outcome_date date,
        outcome int
    )''')

    all_complaint_rows = []
    all_outcome_rows = []

    for entity_id in range(1, 250):
        for year in range(1995, 2000):
            if random.random() < BASE_RATE:
                n_complaints = random.randint(CUTOFF+1, CUTOFF+25)
            else:
                n_complaints = random.randint(0, CUTOFF)
            complaint_rows, outcome_row = collect_entity_year_data(
                entity_id,
                year,
                n_complaints
            )
            all_complaint_rows += complaint_rows
            all_outcome_rows += [outcome_row]
    complaints_fh = io.StringIO()
    writer = csv.writer(complaints_fh)
    for row in all_complaint_rows:
        writer.writerow(row)
    complaints_fh.seek(0)

    outcomes_fh = io.StringIO()
    writer = csv.writer(outcomes_fh)
    for row in all_outcome_rows:
        writer.writerow(row)
    outcomes_fh.seek(0)

    conn = db_engine.raw_connection()
    cur = conn.cursor()
    cur.copy_expert('COPY cat_complaints from stdin CSV ', complaints_fh)
    cur.copy_expert('COPY inspections from stdin CSV', outcomes_fh)
    conn.commit()
    conn.close()


class PipelineLeakageTest(unittest.TestCase):
    def pipeline_leakage_test(self, pipeline_class):
        with testing.postgresql.Postgresql() as postgresql:
            db_engine = create_engine(postgresql.url())
            ensure_db(db_engine)
            populate_source_data(db_engine)
            temporal_config = {
                'beginning_of_time': '1990-01-01',
                'modeling_start_time': '1995-01-01',
                'modeling_end_time': '2001-01-01',
                'update_window': '1year',
                'prediction_window': '1year',
                'look_back_durations': ['1year'],
                'test_durations': ['1year'],
                'prediction_frequency': '1year'
            }
            scoring_config = [
                {'metrics': ['precision@'], 'thresholds': {'top_n': [100]}}
            ]
            grid_config = {
                'sklearn.tree.DecisionTreeClassifier': {
                    'criterion': ['gini'],
                    'max_depth': [1],
                    'min_samples_split': [10],
                }
            }
            feature_config = [{
                'prefix': 'test_features',
                'from_obj': 'cat_complaints',
                'knowledge_date_column': 'as_of_date',
                'aggregates': [{
                    'quantity': 'cat_was_sighted',
                    'metrics': ['count'],
                }],
                'intervals': ['1y'],
                'groups': ['entity_id']
            }]
            experiment_config = {
                'events_table': 'inspections',
                'entity_column_name': 'entity_id',
                'feature_aggregations': feature_config,
                'temporal_config': temporal_config,
                'grid_config': grid_config,
                'scoring': scoring_config,
            }

            pipeline = None
            with TemporaryDirectory() as temp_dir:
                pipeline = pipeline_class(
                    config=experiment_config,
                    db_engine=db_engine,
                    model_storage_class=FSModelStorageEngine,
                    project_path=os.path.join(temp_dir, 'inspections')
                )
                pipeline.run()

            # assert that precision@top100 is perfect
            session = sessionmaker(bind=db_engine)()

            self.assertEqual(session.query(Evaluation).count(), 5)
            for evaluation in session.query(Evaluation).all():
                self.assertEqual(
                    evaluation.value,
                    1.0,
                    'Evaluation for {} is {}'.format(evaluation.evaluation_start_time, evaluation.value)
                )

    def test_serial_pipeline(self):
        self.pipeline_leakage_test(SerialPipeline)


#def test_local_parallel_pipeline():
        #pipeline_leakage_test(LocalParallelPipeline)
