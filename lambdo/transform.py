__author__="Alexandr Savinov"

import numpy as np
import pandas as pd

import logging
log = logging.getLogger('TRANSFORM')


def transform(func, window, data, data_type, model):
    """
    DEPRECATED.
    Apply the specified transformation to the data by producing new column.
    :return: None. The generated columns will be added to the input data frame.
    """

    #
    # Cast to the necessary argument type expected by the function
    #
    if data_type == 'ndarray':
        data_arg = data.values
        data_arg.reshape(-1, 1)
    elif (isinstance(data, pd.DataFrame) and len(data.columns) == 1):
        #data_arg = data
        data_arg = data[data.columns[0]]
    else:
        data_arg = data

    #
    # Apply function depending on the window
    #
    out = None
    if window == 'all':  # Apply function to the whole table

        if isinstance(model, dict):
            out = func(data_arg, **model)
        elif isinstance(model, (list, tuple)):
            out = func(data_arg, *model)
        else:
            out = func(data_arg, model)

    elif window is None or window == 'one' or window == '1':  # Apply function to each row of the table

        #
        # Check if the function is applied to a single column or multiple columns depending on the number of input columns
        #

        if isinstance(data, pd.Series) or ((isinstance(data, pd.DataFrame) and len(data.columns) == 1)):  # Apply to a series. UDF will get single value
            if isinstance(data, pd.DataFrame):
                ser = data[data.columns[0]]
            else:
                ser = data

            #
            # Alternative ways to pass model: 1) flatten JSON 2) as one JSON argument 3) as one custom Python object
            #
            out = pd.Series.apply(ser, func, **model)  # Flatten model. One argument for each key of the model dictinary in UDF has to be declared.

        elif isinstance(data, pd.DataFrame):  # Apply to a frame. UDF will get a row of values
            # Notes:
            # - UDF expects one row as a data input (raw=True - ndarry, raw=False - Series)
            # - model (arguments) cannot be None, so we need to guarantee that we do not pass None

            if data_type == 'ndarray':
                out = pd.DataFrame.apply(data, func, axis=1, raw=True, **model)
            else:
                out = pd.DataFrame.apply(data, func, axis=1, raw=False, **model)

        else:
            return None  # TODO: Either error or implement ndarray and other possible types

    else:  # Apply function to each window of the table
        #
        # Determine window size. The window parameter can be string, number or object (many arguments for rolling object)
        #
        window_size = int(window)
        rolling_args = {'window': window_size}
        # TODO: try/catch with log message if cannot get window size

        #
        # Depending on the number of input columns, the data argument will be either one series or one data frame
        #
        if len(data.columns) == 1:  # Moving aggregation of one input column (sub-series as function argument)

            in_column = data.columns[0]

            # Create a rolling object with windowing (row-based windowing independent of the number of columns)
            by_window = pd.DataFrame.rolling(data, **rolling_args)  # as_index=False - aggregations will produce flat DataFrame instead of Series with index

            # Apply function to all windows
            if data_type == 'ndarray':
                out = by_window[in_column].apply(func, raw=True, **model)
            else:
                out = by_window[in_column].apply(func, raw=False, **model)

        else:  # Moving aggregation of multiple input columns (sub-dataframe as function argument)

            #
            # Workaround: create a new temporary data frame with all row ids, create a rolling object by using it, apply UDF to it, the UDF will get a window/group of row ids which can be then used to access this window rows from the main data frame:
            # Link: https://stackoverflow.com/questions/45928761/rolling-pca-on-pandas-dataframe
            #

            df_idx = pd.DataFrame(np.arange(data.shape[0]))  # Temporary data frame with all row ids like 0,1,2,...
            idx_window = df_idx.rolling(**rolling_args)  # Create rolling object from ids-frame

            # Auxiliary function creates a subframe with data and passes it to the user function
            def window_fn(ids, user_f):
                return user_f(data.iloc[ids])

            out = idx_window.apply(lambda x: window_fn(x, func))

    return out


if __name__ == "__main__":
    pass
