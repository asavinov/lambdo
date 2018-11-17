import unittest

import numpy as np
import pandas as pd

from sklearn import linear_model
from sklearn import ensemble

import json

from lambdo.Workflow import *

#
# Feature functions
#

def price_fn(X):
    """
    Find daily price from Open, High, Low and Close.
    """
    if len(X) < 4: return None
    if not X[0] or not X[1] or not X[2] or not X[3]: return None
    if pd.isna(X[0]) or pd.isna(X[1]) or pd.isna(X[2]) or pd.isna(X[3]): return None
    return (X[0] + X[1] + X[2] + X[3]) / 4.0

def linear_trend_fn(X):
    """Given a Series, fit a linear regression model and return its slope interpreted as a trend"""
    X_array = np.asarray(range(len(X)))
    X_array = X_array.reshape(-1, 1)
    y_array = X.values
    model = linear_model.LinearRegression()
    model.fit(X_array, y_array)
    return model.coef_

def rel_diff_fn(X):
    """
    Return difference between first and second fields of the Series relative to the second.
    """
    if len(X) < 2: return None
    if not X[0] or not X[1]: return None
    if pd.isna(X[0]) or pd.isna(X[1]): return None
    return 100 * (X[0] - X[1]) / X[1]

def ge_fn(value, threshold):
    """
    Return 1 if the value is greater than or equal to the specified threshold, and 0 otherwise
    """
    if value is None or pd.isna(value): return None
    return 1 if value >= threshold else 0

#
# Learning functions
#

# Apply model
def c_predict(X, model):
    X_array = X.values
    y = model.predict(X_array)
    return pd.DataFrame(y, index=X.index)

def lr_fit(X, y, **hyper_model):
    X = X[:-1]
    y = y[:-1]

    X_array = X.values
    y_array = y.values.ravel()

    model = linear_model.LogisticRegression(**hyper_model)

    model.fit(X_array, y_array)

    return model

def gb_fit(X, y, **hyper_model):
    X = X[:-1]
    y = y[:-1]

    X_array = X.values
    y_array = y.values.ravel()

    model = ensemble.GradientBoostingClassifier(**hyper_model)

    model.fit(X_array, y_array)

    return model

def rf_fit(X, y, **hyper_model):
    X = X[:-1]
    y = y[:-1]

    X_array = X.values
    y_array = y.values.ravel()

    model = ensemble.RandomForestClassifier(**hyper_model)
    ensemble.RandomForestClassifier()
    model.fit(X_array, y_array)

    return model


if __name__ == '__main__':
    with open('./examples/example10.json', encoding='utf-8') as f:
        wf_json = json.loads(f.read())
    wf = Workflow(wf_json)
    wf.execute()
    pass
