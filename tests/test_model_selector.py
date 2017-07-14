from triage.model_selectors import ModelSelector, plot_all_best_dist
import testing.postgresql
from sqlalchemy import create_engine
from results_schema.factories import EvaluationFactory, ModelFactory, ModelGroupFactory, init_engine, session
from triage.db import ensure_db
import factory
import numpy


def test_create_model_selector_distance_table():
    with testing.postgresql.Postgresql() as postgresql:
        engine = create_engine(postgresql.url())
        ensure_db(engine)
        init_engine(engine)
        model_groups = {
            'stable': ModelGroupFactory(model_type='myStableClassifier'),
            'bad': ModelGroupFactory(model_type='myBadClassifier'),
            'spiky': ModelGroupFactory(model_type='mySpikeClassifier'),
        }

        class StableModelFactory(ModelFactory):
            model_group_rel = model_groups['stable']

        class BadModelFactory(ModelFactory):
            model_group_rel = model_groups['bad']

        class SpikyModelFactory(ModelFactory):
            model_group_rel = model_groups['spiky']

        models = {
            'stable_3y_ago': StableModelFactory(train_end_time='2014-01-01'),
            'stable_2y_ago': StableModelFactory(train_end_time='2015-01-01'),
            'stable_1y_ago': StableModelFactory(train_end_time='2016-01-01'),
            'bad_3y_ago': BadModelFactory(train_end_time='2014-01-01'),
            'bad_2y_ago': BadModelFactory(train_end_time='2015-01-01'),
            'bad_1y_ago': BadModelFactory(train_end_time='2016-01-01'),
            'spiky_3y_ago': SpikyModelFactory(train_end_time='2014-01-01'),
            'spiky_2y_ago': SpikyModelFactory(train_end_time='2015-01-01'),
            'spiky_1y_ago': SpikyModelFactory(train_end_time='2016-01-01'),
        }

        class ImmediateEvalFactory(EvaluationFactory):
            evaluation_start_time = factory.LazyAttribute(lambda o: o.model_rel.train_end_time)

        class Precision100Factory(ImmediateEvalFactory):
            metric = 'precision@'
            parameter = '100_abs'

        class Recall100Factory(ImmediateEvalFactory):
            metric = 'recall@'
            parameter = '100_abs'

        _ = [
            Precision100Factory(model_rel=models['stable_3y_ago'], value=0.6),
            Precision100Factory(model_rel=models['stable_2y_ago'], value=0.57),
            Precision100Factory(model_rel=models['stable_1y_ago'], value=0.59),
            Precision100Factory(model_rel=models['bad_3y_ago'], value=0.4),
            Precision100Factory(model_rel=models['bad_2y_ago'], value=0.39),
            Precision100Factory(model_rel=models['bad_1y_ago'], value=0.43),
            Precision100Factory(model_rel=models['spiky_3y_ago'], value=0.8),
            Precision100Factory(model_rel=models['spiky_2y_ago'], value=0.4),
            Precision100Factory(model_rel=models['spiky_1y_ago'], value=0.4),
            Recall100Factory(model_rel=models['stable_3y_ago'], value=0.55),
            Recall100Factory(model_rel=models['stable_2y_ago'], value=0.56),
            Recall100Factory(model_rel=models['stable_1y_ago'], value=0.55),
            Recall100Factory(model_rel=models['bad_3y_ago'], value=0.35),
            Recall100Factory(model_rel=models['bad_2y_ago'], value=0.34),
            Recall100Factory(model_rel=models['bad_1y_ago'], value=0.36),
            Recall100Factory(model_rel=models['spiky_3y_ago'], value=0.35),
            Recall100Factory(model_rel=models['spiky_2y_ago'], value=0.8),
            Recall100Factory(model_rel=models['spiky_1y_ago'], value=0.36),
        ]
        session.commit()
        model_selector = ModelSelector(
            db_engine=engine,
            models_table='models',
            distance_table='dist_table'
        )
        metrics = [
            {'metric': 'precision@', 'param': '100_abs'},
            {'metric': 'recall@', 'param': '100_abs'}
        ]
        model_group_ids = [mg.model_group_id for mg in model_groups.values()]
        model_selector.create_and_populate_distance_table(model_group_ids, metrics)

        # get an ordered list of the models/groups for a particular metric/time
        query = '''
            select model_id, raw_value, below_best, below_best_next_time
            from dist_table where metric = %s and parameter = %s and train_end_time = %s
            order by below_best
        '''

        prec_3y_ago = engine.execute(query, ('precision@', '100_abs', '2014-01-01'))
        assert [row for row in prec_3y_ago] == [
            (models['spiky_3y_ago'].model_id, 0.8, 0, 0.17),
            (models['stable_3y_ago'].model_id, 0.6, 0.2, 0),
            (models['bad_3y_ago'].model_id, 0.4, 0.4, 0.18),

        ]

        recall_2y_ago = engine.execute(query, ('recall@', '100_abs', '2015-01-01'))
        assert [row for row in recall_2y_ago] == [
            (models['spiky_2y_ago'].model_id, 0.8, 0, 0.19),
            (models['stable_2y_ago'].model_id, 0.56, 0.24, 0),
            (models['bad_2y_ago'].model_id, 0.34, 0.46, 0.19),

        ]


def test_get_best_diff():
    with testing.postgresql.Postgresql() as postgresql:
        engine = create_engine(postgresql.url())
        ensure_db(engine)
        init_engine(engine)
        model_selector = ModelSelector(
            db_engine=engine,
            models_table='models',
            distance_table='dist_table',
        )
        model_selector._create_distance_table()
        distance_rows = [
            (1, 1, '2014-01-01', 'precision@', '100_abs', 0.5, 0.1, 0.15),
            (1, 2, '2015-01-01', 'precision@', '100_abs', 0.5, 0.15, 0.18),
            (1, 3, '2016-01-01', 'precision@', '100_abs', 0.46, 0.21, 0.11),
            (2, 4, '2014-01-01', 'precision@', '100_abs', 0.5, 0.1, 0.15),
            (2, 5, '2015-01-01', 'precision@', '100_abs', 0.5, 0.15, 0.18),
            (2, 6, '2016-01-01', 'precision@', '100_abs', 0.46, 0.21, 0.11),
        ]
        for dist_row in distance_rows:
            engine.execute(
                'insert into dist_table values (%s, %s, %s, %s, %s, %s, %s, %s)',
                dist_row
            )
        df_dist = model_selector.get_best_dist(
            metric='precision@',
            metric_param='100_abs',
            max_below_best=0.2,
            model_group_ids=[1]
        )
        # make sure we have four columns and a row for each % diff value
        assert df_dist.shape == (21, 4)
        assert numpy.isclose(
            df_dist[df_dist['pct_diff'] == 0.20]['pct_of_time'].values[0],
            1.0
        )
        assert numpy.isclose(
            df_dist[df_dist['pct_diff'] == 0.11]['pct_of_time'].values[0],
            0.5
        )
        assert numpy.isclose(
            df_dist[df_dist['pct_diff'] == 0.05]['pct_of_time'].values[0],
            0.0
        )


def test_model_groups_past_threshold_as_of():
    with testing.postgresql.Postgresql() as postgresql:
        engine = create_engine(postgresql.url())
        ensure_db(engine)
        init_engine(engine)
        model_selector = ModelSelector(
            db_engine=engine,
            models_table='models',
            distance_table='dist_table',
        )
        model_selector._create_distance_table()
        distance_rows = [
            # model group 1 should pass
            (1, 1, '2014-01-01', 'precision@', '100_abs', 0.5, 0.0, 0.38),
            (1, 2, '2015-01-01', 'precision@', '100_abs', 0.5, 0.38, 0.0),
            (1, 3, '2016-01-01', 'precision@', '100_abs', 0.46, 0.0, 0.11),
            # model group 2 should not pass because the min value never passes
            (2, 4, '2014-01-01', 'precision@', '100_abs', 0.39, 0.11, 0.5),
            (2, 5, '2015-01-01', 'precision@', '100_abs', 0.38, 0.5, 0.12),
            (2, 6, '2016-01-01', 'precision@', '100_abs', 0.34, 0.12, 0.11),
            # model group 3 not included in this round
            (3, 7, '2014-01-01', 'precision@', '100_abs', 0.28, 0.22, 0.0),
            (3, 8, '2015-01-01', 'precision@', '100_abs', 0.88, 0.0, 0.02),
            (3, 9, '2016-01-01', 'precision@', '100_abs', 0.44, 0.02, 0.11),
            # model group 4 should not pass because it is never that close
            (4, 10, '2014-01-01', 'precision@', '100_abs', 0.29, 0.21, 0.21),
            (4, 11, '2015-01-01', 'precision@', '100_abs', 0.67, 0.21, 0.21),
            (4, 12, '2016-01-01', 'precision@', '100_abs', 0.25, 0.21, 0.21),
        ]
        for dist_row in distance_rows:
            engine.execute(
                'insert into dist_table values (%s, %s, %s, %s, %s, %s, %s, %s)',
                dist_row
            )
        model_groups = model_selector.model_groups_past_threshold_as_of(
            metric_filters=[
                {
                    'metric': 'precision@',
                    'metric_param': '100_abs',
                    'max_below_best': 0.2,
                    'min_value': 0.4,
                },
            ],
            model_group_ids=[1, 2, 4],
            train_end_time='2014-01-01',
        )
        assert model_groups == set([1])


def test_get_all_best_diffs():
    with testing.postgresql.Postgresql() as postgresql:
        engine = create_engine(postgresql.url())
        ensure_db(engine)
        init_engine(engine)
        model_groups = {
            'stable': ModelGroupFactory(model_type='myStableClassifier'),
        }

        class StableModelFactory(ModelFactory):
            model_group_rel = model_groups['stable']


        models = {
            'stable_3y_ago': StableModelFactory(train_end_time='2014-01-01'),
            'stable_2y_ago': StableModelFactory(train_end_time='2015-01-01'),
            'stable_1y_ago': StableModelFactory(train_end_time='2016-01-01'),
        }
        session.commit()
        model_selector = ModelSelector(
            db_engine=engine,
            models_table='models',
            distance_table='dist_table',
        )
        model_selector._create_distance_table()
        distance_rows = [
            (
                model_groups['stable'].model_group_id,
                models['stable_3y_ago'].model_id,
                models['stable_3y_ago'].train_end_time,
                'precision@',
                '100_abs',
                0.5,
                0.1,
                0.15
            ),
            (
                model_groups['stable'].model_group_id,
                models['stable_2y_ago'].model_id,
                models['stable_2y_ago'].train_end_time,
                'precision@',
                '100_abs',
                0.5,
                0.15,
                0.18
            ),
            (
                model_groups['stable'].model_group_id,
                models['stable_1y_ago'].model_id,
                models['stable_1y_ago'].train_end_time,
                'precision@',
                '100_abs',
                0.46,
                0.21,
                0.11
            ),
            (
                model_groups['stable'].model_group_id,
                models['stable_3y_ago'].model_id,
                models['stable_3y_ago'].train_end_time,
                'recall@',
                '100_abs',
                0.3,
                0.4,
                0.42
            ),
            (
                model_groups['stable'].model_group_id,
                models['stable_2y_ago'].model_id,
                models['stable_2y_ago'].train_end_time,
                'recall@',
                '100_abs',
                0.34,
                0.34,
                0.32
            ),
            (
                model_groups['stable'].model_group_id,
                models['stable_1y_ago'].model_id,
                models['stable_1y_ago'].train_end_time,
                'recall@',
                '100_abs',
                0.32,
                0.36,
                0.35
            )
        ]
        for dist_row in distance_rows:
            engine.execute(
                'insert into dist_table values (%s, %s, %s, %s, %s, %s, %s, %s)',
                dist_row
            )

        metrics_with_dfs = model_selector.get_all_distance_matrices(
            metrics=[
                {'metric': 'precision@', 'metric_param': '100_abs', 'max_below_best': 0.2},
                {'metric': 'recall@', 'metric_param': '100_abs', 'max_below_best': 0.35},
            ]
        )
        for metric_with_df in metrics_with_dfs:
            assert 'distance_matrix' in metric_with_df
        assert metrics_with_dfs[0]['metric'] == 'precision@'
        assert metrics_with_dfs[0]['distance_matrix'].shape == (21, 4)
        assert metrics_with_dfs[1]['metric'] == 'recall@'
        assert metrics_with_dfs[1]['distance_matrix'].shape == (36, 4)



def test_calculate_regrets():
    with testing.postgresql.Postgresql() as postgresql:
        engine = create_engine(postgresql.url())
        ensure_db(engine)
        init_engine(engine)
        model_groups = {
            'stable': ModelGroupFactory(model_type='myStableClassifier'),
            'spiky': ModelGroupFactory(model_type='mySpikeClassifier'),
        }

        class StableModelFactory(ModelFactory):
            model_group_rel = model_groups['stable']

        class SpikyModelFactory(ModelFactory):
            model_group_rel = model_groups['spiky']

        models = {
            'stable_3y_ago': StableModelFactory(train_end_time='2014-01-01'),
            'stable_2y_ago': StableModelFactory(train_end_time='2015-01-01'),
            'stable_1y_ago': StableModelFactory(train_end_time='2016-01-01'),
            'spiky_3y_ago': SpikyModelFactory(train_end_time='2014-01-01'),
            'spiky_2y_ago': SpikyModelFactory(train_end_time='2015-01-01'),
            'spiky_1y_ago': SpikyModelFactory(train_end_time='2016-01-01'),
        }
        session.commit()
        model_selector = ModelSelector(
            db_engine=engine,
            models_table='models',
            distance_table='dist_table'
        )
        model_selector._create_distance_table()
        distance_rows = [
            (
                model_groups['stable'].model_group_id,
                models['stable_3y_ago'].model_id,
                models['stable_3y_ago'].train_end_time,
                'precision@',
                '100_abs',
                0.5,
                0.1,
                0.15
            ),
            (
                model_groups['stable'].model_group_id,
                models['stable_2y_ago'].model_id,
                models['stable_2y_ago'].train_end_time,
                'precision@',
                '100_abs',
                0.5,
                0.15,
                0.18
            ),
            (
                model_groups['stable'].model_group_id,
                models['stable_1y_ago'].model_id,
                models['stable_1y_ago'].train_end_time,
                'precision@',
                '100_abs',
                0.46,
                0.21,
                0.11
            ),
            (
                model_groups['spiky'].model_group_id,
                models['spiky_3y_ago'].model_id,
                models['spiky_3y_ago'].train_end_time,
                'precision@',
                '100_abs',
                0.45,
                0.15,
                0.19
            ),
            (
                model_groups['spiky'].model_group_id,
                models['spiky_2y_ago'].model_id,
                models['spiky_2y_ago'].train_end_time,
                'precision@',
                '100_abs',
                0.84,
                0.0,
                0.3
            ),
            (
                model_groups['spiky'].model_group_id,
                models['spiky_1y_ago'].model_id,
                models['spiky_1y_ago'].train_end_time,
                'precision@',
                '100_abs',
                0.45,
                0.22,
                0.12
            ),
        ]
        for dist_row in distance_rows:
            engine.execute(
                'insert into dist_table values (%s, %s, %s, %s, %s, %s, %s, %s)',
                dist_row
            )

        def pick_spiky(metric, metric_param, df):
            return model_groups['spiky'].model_group_id

        regrets = model_selector.calculate_regrets(
            selection_rule=pick_spiky,
            model_group_ids=[mg.model_group_id for mg in model_groups.values()],
            train_end_times=['2014-01-01', '2015-01-01', '2016-01-01'],
            metric='precision@',
            metric_param='100_abs',
        )
        assert regrets == [0.19, 0.3, 0.12]
