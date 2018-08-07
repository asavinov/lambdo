import unittest

import numpy as np
import pandas as pd
from sklearn import linear_model

import json

from lambdo.Workflow import *

def diff_fn(X):
    """
    Return difference between first and second fields of the Series.
    """
    if len(X) < 2: return None
    if not X[0] or not X[1]: return None
    if pd.isna(X[0]) or pd.isna(X[1]): return None
    return X[0] - X[1]

def linear_trend_fn(X):
    """Given a Series of certain size, fit a linear regression model and return its slope as a trend"""
    X_array = np.asarray(range(len(X)))
    X_array = X_array.reshape(-1, 1)
    y_array = X.values
    model = linear_model.LinearRegression()
    model.fit(X_array, y_array)
    return model.coef_


if __name__ == '__main__':
    with open('../examples/example1.json', encoding='utf-8') as f:
        wf_json = json.loads(f.read())
    wf = Workflow(wf_json)
    wf.execute()
    pass
