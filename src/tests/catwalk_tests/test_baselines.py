import numpy as np
import pandas as pd

import pytest

from triage.component.catwalk.baselines.rankers import PercentileRankOneFeature


@pytest.fixture
def data():
    X_train = pd.DataFrame({
        'x1': [0,  1, 2, 56, 25, 8, -3, 89],
        'x2': [0, 23, 1,  6,  5, 3, 18,  7],
        'x3': [1, 12, 5, -6,  2, 5,  3, -3],
        'x4': [6, 13, 4,  5, 35, 6, 43, 74]
    })
    y_train = [0,  1, 0,  1,  1, 1,  3,  0]
    X_test = pd.DataFrame({
        'x1': [4, 14, 2,  6, 25, 8, -3,  4],
        'x2': [6, -1, 2, 24,  5, 3, 18, 39],
        'x3': [1,  7, 4, 57,  2, 5,  3,  2],
        'x4': [7,  3, 6, 39, 35, 6, 43, -6]
    })
    y_test  = [1,  3, 0,  0,  0, 0,  0,  1]

    return {'X_train':X_train, 'X_test':X_test, 'y_train':y_train, 'y_test':y_test}

def test_fit(data):
    ranker = PercentileRankOneFeature(feature='x3')
    assert ranker.feature_importances_ == None
    ranker.fit(x=data['X_train'], y=data['y_train'])
    assert ranker.feature_importances_ == [0, 0, 1, 0] 

def test_predict_proba(data):
    for descend_value in [True, False]:
        ranker = PercentileRankOneFeature(feature='x3', descend=descend_value)
        ranker.fit(x=data['X_train'], y=data['y_train'])
        results = ranker.predict_proba(data['X_test'])
        if descend_value:
            expected_results = np.array(
                [np.zeros(len(data['X_test'])), [.875, .125, .375, 0, .625, .25, .5, .625]]
            ).transpose()
        else:
            expected_results = np.array(
                [np.zeros(len(data['X_test'])), [0,  .75, .5, .875, .125, .625, .375, .125]]
            ).transpose()
        np.testing.assert_array_equal(results, expected_results)
