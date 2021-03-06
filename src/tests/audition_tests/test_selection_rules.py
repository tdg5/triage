import pandas

from triage.component.audition.selection_rules import (
    best_current_value,
    best_average_value,
    most_frequent_best_dist,
    best_average_two_metrics,
    best_avg_var_penalized,
    best_avg_recency_weight,
    lowest_metric_variance,
)


def test_best_current_value_greater_is_better():
    df = pandas.DataFrame.from_dict({
        'model_group_id': ['1', '2', '1', '2'],
        'model_id': ['1', '2', '3', '4'],
        'train_end_time': ['2011-01-01', '2011-01-01', '2012-01-01', '2012-01-01'],
        'metric': ['precision@', 'precision@', 'precision@', 'precision@'],
        'parameter': ['100_abs', '100_abs', '100_abs', '100_abs'],
        'raw_value': [0.5, 0.4, 0.6, 0.7],
        'dist_from_best_case': [0.0, 0.1, 0.1, 0.0],
    })

    assert best_current_value(df, '2011-01-01', 'precision@', '100_abs') == '1'
    assert best_current_value(df, '2012-01-01', 'precision@', '100_abs') == '2'


def test_best_current_value_lesser_is_better():
    df = pandas.DataFrame.from_dict({
        'model_group_id': ['1', '2', '1', '2'],
        'model_id': ['1', '2', '3', '4'],
        'train_end_time': ['2011-01-01', '2011-01-01', '2012-01-01', '2012-01-01'],
        'metric': ['false positives@', 'false positives@', 'false positives@', 'false positives@'],
        'parameter': ['100_abs', '100_abs', '100_abs', '100_abs'],
        'raw_value': [40, 50, 60, 70],
        'dist_from_best_case': [0, 10, 0, 10],
    })

    assert best_current_value(df, '2011-01-01', 'false positives@', '100_abs') == '1'
    assert best_current_value(df, '2012-01-01', 'false positives@', '100_abs') == '1'


def test_best_average_value_greater_is_better():
    df = pandas.DataFrame.from_dict({
        'model_group_id': ['1', '2', '1', '2', '1', '2'],
        'model_id': ['1', '2', '3', '4', '5', '6'],
        'train_end_time': ['2011-01-01', '2011-01-01', '2012-01-01', '2012-01-01', '2013-01-01', '2013-01-01'],
        'metric': ['precision@', 'precision@', 'precision@', 'precision@', 'precision@', 'precision@'],
        'parameter': ['100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs'],
        'raw_value': [0.5, 0.4, 0.6, 0.69, 0.6, 0.62],
        'dist_from_best_case': [0.0, 0.1, 0.1, 0.0, 0.02, 0.0],
    })

    assert best_average_value(df, '2013-01-01', 'precision@', '100_abs') == '2'


def test_best_average_value_lesser_is_better():
    df = pandas.DataFrame.from_dict({
        'model_group_id': ['1', '2', '1', '2', '1', '2'],
        'model_id': ['1', '2', '3', '4', '5', '6'],
        'train_end_time': ['2011-01-01', '2011-01-01', '2012-01-01', '2012-01-01', '2013-01-01', '2013-01-01'],
        'metric': ['false positives@', 'false positives@', 'false positives@', 'false positives@', 'false positives@', 'false positives@'],
        'parameter': ['100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs'],
        'raw_value': [20, 30, 30, 30, 10, 5],
        'dist_from_best_case': [0, 10, 0, 0, 5, 0],
    })

    assert best_average_value(df, '2013-01-01', 'false positives@', '100_abs') == '1'


def test_most_frequent_best_dist():
    df = pandas.DataFrame.from_dict({
        'model_group_id': ['1', '2', '1', '2', '1', '2'],
        'model_id': ['1', '2', '3', '4', '5', '6'],
        'train_end_time': ['2011-01-01', '2011-01-01', '2012-01-01', '2012-01-01', '2013-01-01', '2013-01-01'],
        'metric': ['precision@', 'precision@', 'precision@', 'precision@', 'precision@', 'precision@'],
        'parameter': ['100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs'],
        'raw_value': [0.5, 0.4, 0.6, 0.69, 0.6, 0.62],
        'dist_from_best_case': [0.0, 0.1, 0.1, 0.0, 0.02, 0.0],
    })

    assert most_frequent_best_dist(df, '2013-01-01', 'precision@', '100_abs', 0.01) == '2'


def test_best_average_two_metrics_greater_is_better():
    df = pandas.DataFrame.from_dict({
        'model_group_id': ['1', '1', '2', '2', '1', '1', '2', '2'],
        'model_id': ['1', '1', '2', '2', '3', '3', '4', '4'],
        'train_end_time': ['2011-01-01', '2011-01-01', '2011-01-01', '2011-01-01', '2012-01-01', '2012-01-01', '2012-01-01', '2012-01-01'],
        'metric': ['precision@', 'recall@', 'precision@', 'recall@', 'precision@', 'recall@', 'precision@', 'recall@'],
        'parameter': ['100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs'],
        'raw_value': [0.6, 0.4, 0.4, 0.6, 0.5, 0.5, 0.4, 0.5],
        'dist_from_best_case': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    })

    assert best_average_two_metrics(df, '2013-01-01', 'precision@', '100_abs', 'recall@', '100_abs', 0.5) == '1'
    assert best_average_two_metrics(df, '2013-01-01', 'precision@', '100_abs', 'recall@', '100_abs', 0.1) == '2'


def test_best_average_two_metrics_lesser_is_better():
    df = pandas.DataFrame.from_dict({
        'model_group_id': ['1', '1', '2', '2', '1', '1', '2', '2'],
        'model_id': ['1', '1', '2', '2', '3', '3', '4', '4'],
        'train_end_time': ['2011-01-01', '2011-01-01', '2011-01-01', '2011-01-01', '2012-01-01', '2012-01-01', '2012-01-01', '2012-01-01'],
        'metric': ['false positives@', 'false negatives@', 'false positives@', 'false negatives@', 'false positives@', 'false negatives@', 'false positives@', 'false negatives@'],
        'parameter': ['100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs'],
        'raw_value': [20, 30, 40, 20, 20, 30, 40, 20],
        'dist_from_best_case': [0, 10, 20, 0, 0, 10, 20, 0],
    })

    assert best_average_two_metrics(df, '2013-01-01', 'false positives@', '100_abs', 'false negatives@', '100_abs', 0.5) == '1'
    assert best_average_two_metrics(df, '2013-01-01', 'false positives@', '100_abs', 'false negatives@', '100_abs', 0.1) == '2'


def test_lowest_metric_variance():
    df = pandas.DataFrame.from_dict({
        'model_group_id': ['1', '2', '1', '2', '1', '2'],
        'model_id': ['1', '2', '3', '4', '5', '6'],
        'train_end_time': ['2011-01-01', '2011-01-01', '2012-01-01', '2012-01-01', '2013-01-01', '2013-01-01'],
        'metric': ['precision@', 'precision@', 'precision@', 'precision@', 'precision@', 'precision@'],
        'parameter': ['100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs'],
        'raw_value': [0.5, 0.5, 0.6, 0.9, 0.4, 0.2],
        'dist_from_best_case': [0.0, 0.0, 0.3, 0.0, 0.0, 0.2],
    })

    assert lowest_metric_variance(df, '2013-01-01', 'precision@', '100_abs') == '1'


def test_best_avg_var_penalized_greater_is_better():
    df = pandas.DataFrame.from_dict({
        'model_group_id': ['1', '2', '1', '2', '1', '2'],
        'model_id': ['1', '2', '3', '4', '5', '6'],
        'train_end_time': ['2011-01-01', '2011-01-01', '2012-01-01', '2012-01-01', '2013-01-01', '2013-01-01'],
        'metric': ['precision@', 'precision@', 'precision@', 'precision@', 'precision@', 'precision@'],
        'parameter': ['100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs'],
        'raw_value': [0.5, 0.4, 0.8, 0.3, 0.2, 0.5],
        'dist_from_best_case': [0.0, 0.1, 0.0, 0.5, 0.3, 0.0],
    })

    assert best_avg_var_penalized(df, '2013-01-01', 'precision@', '100_abs', 0.5) == '1'
    assert best_avg_var_penalized(df, '2013-01-01', 'precision@', '100_abs', 1.0) == '2'


def test_best_avg_var_penalized_lesser_is_better():
    df = pandas.DataFrame.from_dict({
        'model_group_id': ['1', '2', '1', '2', '1', '2'],
        'model_id': ['1', '2', '3', '4', '5', '6'],
        'train_end_time': ['2011-01-01', '2011-01-01', '2012-01-01', '2012-01-01', '2013-01-01', '2013-01-01'],
        'metric': ['false positives@', 'false positives@', 'false positives@', 'false positives@', 'false positives@', 'false positives@'],
        'parameter': ['100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs'],
        'raw_value': [40, 50, 10, 60, 70, 40],
        'dist_from_best_case': [0, 10, 0, 50, 30, 0],
    })

    assert best_avg_var_penalized(df, '2013-01-01', 'false positives@', '100_abs', 0.2) == '1'
    assert best_avg_var_penalized(df, '2013-01-01', 'false positives@', '100_abs', 0.7) == '2'


def test_best_avg_recency_weight_greater_is_better():
    df = pandas.DataFrame.from_dict({
        'model_group_id': ['1', '2', '1', '2', '1', '2'],
        'model_id': ['1', '2', '3', '4', '5', '6'],
        'train_end_time': ['2011-01-01', '2011-01-01', '2012-01-01', '2012-01-01', '2013-01-01', '2013-01-01'],
        'metric': ['precision@', 'precision@', 'precision@', 'precision@', 'precision@', 'precision@'],
        'parameter': ['100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs'],
        'raw_value': [0.8, 0.2, 0.5, 0.5, 0.2, 0.7],
        'dist_from_best_case': [0.0, 0.6, 0.0, 0.0, 0.5, 0.0],
    })
    df['train_end_time'] = pandas.to_datetime(df['train_end_time'])

    assert best_avg_recency_weight(df, '2013-01-01', 'precision@', '100_abs', 1.00, 'linear') == '1'
    assert best_avg_recency_weight(df, '2013-01-01', 'precision@', '100_abs', 1.15, 'linear') == '1'
    assert best_avg_recency_weight(df, '2013-01-01', 'precision@', '100_abs', 1.50, 'linear') == '2'


def test_best_avg_recency_weight_lesser_is_better():
    df = pandas.DataFrame.from_dict({
        'model_group_id': ['1', '2', '1', '2', '1', '2'],
        'model_id': ['1', '2', '3', '4', '5', '6'],
        'train_end_time': ['2011-01-01', '2011-01-01', '2012-01-01', '2012-01-01', '2013-01-01', '2013-01-01'],
        'metric': ['false positives@', 'false positives@', 'false positives@', 'false positives@', 'false positives@', 'false positives@'],
        'parameter': ['100_abs', '100_abs', '100_abs', '100_abs', '100_abs', '100_abs'],
        'raw_value': [20, 90, 50, 50, 80, 20],
        'dist_from_best_case': [60, 0, 0, 0, 0, 70],
    })
    df['train_end_time'] = pandas.to_datetime(df['train_end_time'])

    assert best_avg_recency_weight(df, '2013-01-01', 'false positives@', '100_abs', 1.00, 'linear') == '1'
    assert best_avg_recency_weight(df, '2013-01-01', 'false positives@', '100_abs', 1.15, 'linear') == '1'
    assert best_avg_recency_weight(df, '2013-01-01', 'false positives@', '100_abs', 1.50, 'linear') == '2'
