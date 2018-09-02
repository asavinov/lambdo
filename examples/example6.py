import pandas as pd
from sklearn import ensemble

def diff_fn(X):
    """Difference between first and second fields of the input Series."""
    if len(X) < 2: return None
    if not X[0] or not X[1]: return None
    if pd.isna(X[0]) or pd.isna(X[1]): return None
    return X[0] - X[1]

def gb_fit(X, y, **hyper_model):
    """Fit a gradient boosting model using a standard function. It is a wrapper function."""

    # Create model object using hyper parameters (if any)
    model = ensemble.GradientBoostingRegressor(**hyper_model)

    # Fit model
    model.fit(X, y.ravel())

    # Return model
    return model

def gb_predict(X, model):
    """Apply a gradient boosting model using a standard function. It is a wrapper function."""
    y = model.predict(X)
    return y
