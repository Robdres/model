# test_train.py
import pytest
import numpy as np
import pandas as pd
from train import (
    stratified_split_allowing_singletons,
    build_preprocessor,
    train_and_evaluate,
)


def test_stratified_split_allowing_singletons():
    # 4 classes, at least 2 samples per class, plus one singleton
    X = pd.DataFrame({'a': range(13)})
    # class 5 is singleton
    y = np.array([0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 4, 4, 5])
    # test_size=0.4 gives 5 samples in test set (enough for 4 classes)
    X_train, X_test, y_train, y_test = stratified_split_allowing_singletons(
        X, y, test_size=0.4, random_state=1)
    assert 5 in y_train and 5 not in y_test
    assert set(y_test).issubset({0, 1, 2, 3, 4})


def test_build_preprocessor():
    X = pd.DataFrame({
        'num': [1.0, 2.0, 3.0],
        'cat': ['a', 'b', 'c']
    })
    preprocess, numeric, categorical = build_preprocessor(X)
    assert 'num' in numeric
    assert 'cat' in categorical


def test_train_and_evaluate_runs():
    X = pd.DataFrame({
        'num': np.random.rand(20),
        'cat': ['a', 'b'] * 10
    })
    y = np.random.randint(0, 5, size=20)
    preprocess, _, _ = build_preprocessor(X)
    records, best_name, best_estimator, best_score = train_and_evaluate(
        X, X, y, y, preprocess)
    assert isinstance(records, list)
    assert best_name is not None
    assert best_estimator is not None
