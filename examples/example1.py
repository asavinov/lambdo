import unittest

import numpy as np
import pandas as pd

import json

from lambdo.Workflow import *

def diff_func(X):
    """
    Difference between high and low prices
    """
    if not X[0] or not X[0]: return None
    if pd.isna(X[0]) or pd.isna(X[0]): return None
    return X[1] - X[0]

if __name__ == '__main__':
    with open('../examples/example1.jsonc', encoding='utf-8') as f:
        wf_json = json.loads(f.read())
    wf = Workflow(wf_json)
    wf.execute()
    pass
