from datetime import datetime
import tempfile

import factory
import testing.postgresql
import yaml
from sqlalchemy import create_engine

from triage.component.audition import Auditioner
from triage.component.catwalk.db import ensure_db

from tests.results_tests.factories import (
    EvaluationFactory,
    ModelFactory,
    ModelGroupFactory,
    init_engine,
    session,
)


def test_Auditioner():
    with testing.postgresql.Postgresql() as postgresql:
        db_engine = create_engine(postgresql.url())
        ensure_db(db_engine)
        init_engine(db_engine)
        # set up data, randomly generated by the factories but conforming
        # generally to what we expect results schema data to look like
        num_model_groups = 10
        model_types = [
            'classifier type {}'.format(i)
            for i in range(0, num_model_groups)
        ]
        model_groups = [
            ModelGroupFactory(model_type=model_type)
            for model_type in model_types
        ]
        train_end_times = [
            datetime(2013, 1, 1),
            datetime(2014, 1, 1),
            datetime(2015, 1, 1),
            datetime(2016, 1, 1),
        ]
        models = [
            ModelFactory(model_group_rel=model_group, train_end_time=train_end_time)
            for model_group in model_groups
            for train_end_time in train_end_times
        ]
        metrics = [
            ('precision@', '100_abs'),
            ('recall@', '100_abs'),
            ('precision@', '50_abs'),
            ('recall@', '50_abs'),
            ('fpr@', '10_pct'),
        ]

        class ImmediateEvalFactory(EvaluationFactory):
            evaluation_start_time = factory.LazyAttribute(lambda o: o.model_rel.train_end_time)

        for model in models:
            for (metric, parameter) in metrics:
                ImmediateEvalFactory(model_rel=model,
                                     metric=metric,
                                     parameter=parameter)

        session.commit()

        # define a very loose filtering that should admit all model groups
        no_filtering = [
            {
                'metric': 'precision@',
                'parameter': '100_abs',
                'max_from_best': 1.0,
                'threshold_value': 0.0
            },
            {
                'metric': 'recall@',
                'parameter': '100_abs',
                'max_from_best': 1.0,
                'threshold_value': 0.0
            }
        ]
        model_group_ids = [mg.model_group_id for mg in model_groups]
        auditioner = Auditioner(
            db_engine,
            model_group_ids,
            train_end_times,
            no_filtering,
        )
        assert len(auditioner.thresholded_model_group_ids) == num_model_groups
        auditioner.plot_model_groups()


        # here, we pick thresholding rules that should definitely remove
        # all model groups from contention because they are too strict.
        remove_all = [
            {
                'metric': 'precision@',
                'parameter': '100_abs',
                'max_from_best': 0.0,
                'threshold_value': 1.1
            },
            {
                'metric': 'recall@',
                'parameter': '100_abs',
                'max_from_best': 0.0,
                'threshold_value': 1.1
            }
        ]

        auditioner.update_metric_filters(new_filters=remove_all)
        assert len(auditioner.thresholded_model_group_ids) == 0

        # pass the argument instead and remove all model groups
        auditioner.update_metric_filters(
            metric='precision@',
            parameter='100_abs',
            max_from_best=0.0,
            threshold_value=1.1)
        assert len(auditioner.thresholded_model_group_ids) == 0

        # one potential place for bugs would be when we pull back the rules
        # for being too restrictive. we want to make sure that the original list is
        # always used for thresholding, or else such a move would be impossible
        auditioner.update_metric_filters(new_filters=no_filtering)
        assert len(auditioner.thresholded_model_group_ids) == num_model_groups

        # pass the argument instead and let all model groups pass
        auditioner.update_metric_filters(
            metric='precision@',
            parameter='100_abs',
            max_from_best=1.0,
            threshold_value=0.0)
        assert len(auditioner.thresholded_model_group_ids) == num_model_groups

        # now, we want to take this partially thresholded list and run it through
        # a grid of selection rules, meant to pick winners by a variety of user-defined
        # criteria
        rule_grid = [{
            'shared_parameters': [
                {'metric': 'precision@', 'parameter': '100_abs'},
                {'metric': 'recall@', 'parameter': '100_abs'},
            ],
            'selection_rules': [
                {'name': 'most_frequent_best_dist', 'dist_from_best_case': [0.1, 0.2, 0.3]},
                {'name': 'best_current_value'}
            ]
        }, {
            'shared_parameters': [
                {'metric1': 'precision@', 'parameter1': '100_abs'},
            ],
            'selection_rules': [
                {
                    'name': 'best_average_two_metrics',
                    'metric2': ['recall@'],
                    'parameter2': ['100_abs'],
                    'metric1_weight': [0.4, 0.5, 0.6]
                },
            ]
        }]
        auditioner.register_selection_rule_grid(rule_grid, plot=False)
        final_model_group_ids = auditioner.selection_rule_model_group_ids

        # we expect the result to be a mapping of selection rule name to model group id
        assert isinstance(final_model_group_ids, dict)

        # we expect that there is one winner for each selection rule
        assert sorted(final_model_group_ids.keys()) == \
            sorted([rule.descriptive_name for rule in auditioner.selection_rules])

        # we expect that the results written to the yaml file are the
        # chosen model groups and their rules
        # however because the source data is randomly generated we could have a
        # different list on consecutive runs
        # and don't want to introduce non-determinism to the test
        with tempfile.NamedTemporaryFile() as tf:
            auditioner.write_tyra_config(tf.name)
            assert sorted(yaml.load(tf)['selection_rule_model_groups'].keys()) == \
                sorted(final_model_group_ids.keys())
