import pandas as pd

def diff_fn(X):
    """
    Difference between first and second fields of the input Series.
    """
    if len(X) < 2: return None
    if not X[0] or not X[1]: return None
    if pd.isna(X[0]) or pd.isna(X[1]): return None
    return X[0] - X[1]
