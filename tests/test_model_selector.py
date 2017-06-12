from triage.model_selectors import ModelSelector
import testing.postgresql
from sqlalchemy import create_engine
from results_schema.factories import EvaluationFactory, ModelFactory, ModelGroupFactory, init_engine, session
from triage.db import ensure_db
import factory


def test_create_model_selector_table():
    with testing.postgresql.Postgresql() as postgresql:
        engine = create_engine(postgresql.url())
        ensure_db(engine)
        init_engine(engine)
        model_groups = {
            'great': ModelGroupFactory(model_type='myGreatClassifier'),
            'bad': ModelGroupFactory(model_type='myBadClassifier'),
            'spiky': ModelGroupFactory(model_type='mySpikeClassifier'),
        }

        class GreatModelFactory(ModelFactory):
            model_group_rel = model_groups['great']

        class BadModelFactory(ModelFactory):
            model_group_rel = model_groups['bad']

        class SpikyModelFactory(ModelFactory):
            model_group_rel = model_groups['spiky']

        models = {
            'great_3y_ago': GreatModelFactory(train_end_time='2014-01-01'),
            'great_2y_ago': GreatModelFactory(train_end_time='2015-01-01'),
            'great_1y_ago': GreatModelFactory(train_end_time='2016-01-01'),
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
            Precision100Factory(model_rel=models['great_3y_ago'], value=0.6),
            Precision100Factory(model_rel=models['great_2y_ago'], value=0.57),
            Precision100Factory(model_rel=models['great_1y_ago'], value=0.59),
            Precision100Factory(model_rel=models['bad_3y_ago'], value=0.4),
            Precision100Factory(model_rel=models['bad_2y_ago'], value=0.39),
            Precision100Factory(model_rel=models['bad_1y_ago'], value=0.43),
            Precision100Factory(model_rel=models['spiky_3y_ago'], value=0.8),
            Precision100Factory(model_rel=models['spiky_2y_ago'], value=0.4),
            Precision100Factory(model_rel=models['spiky_1y_ago'], value=0.4),
            Recall100Factory(model_rel=models['great_3y_ago'], value=0.55),
            Recall100Factory(model_rel=models['great_2y_ago'], value=0.56),
            Recall100Factory(model_rel=models['great_1y_ago'], value=0.55),
            Recall100Factory(model_rel=models['bad_3y_ago'], value=0.35),
            Recall100Factory(model_rel=models['bad_2y_ago'], value=0.34),
            Recall100Factory(model_rel=models['bad_1y_ago'], value=0.36),
            Recall100Factory(model_rel=models['spiky_3y_ago'], value=0.35),
            Recall100Factory(model_rel=models['spiky_2y_ago'], value=0.8),
            Recall100Factory(model_rel=models['spiky_1y_ago'], value=0.36),
        ]
        session.commit()
        model_selector = ModelSelector(db_engine=engine)
        metrics = ['precision@100_abs', 'recall@100_abs']
        model_group_ids = [mg.model_group_id for mg in model_groups.values()]
        output_table = model_selector.create_table(model_group_ids, metrics)
        query = '''
            select model_id, below_max, below_max_next_time
            from {} where metric = '%s' and test_set = '%s'
            order by below_max desc
        '''.format(output_table)
        prec_3y_ago = engine.execute(query, ('precision@100_abs', '2014-01-01'))
        assert [row for row in prec_3y_ago] == [
            (models['spiky_3y_ago'], 0, 0.17),
            (models['great_3y_ago'], 0.2, 0),
            (models['bad_3y_ago'], 0.4, 0.21),

        ]

        recall_2y_ago = engine.execute(query, ('recall@100_abs', '2015-01-01'))
        assert [row for row in recall_3y_ago] == [
            (models['spiky_2y_ago'], 0, 0.2),
            (models['great_2y_ago'], 0.24, 0),
            (models['bad_2y_ago'], 0.46, 0.19),

        ]
