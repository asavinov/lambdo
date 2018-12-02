__author__="Alexandr Savinov"

import os
import pickle
import urllib.parse

import pandas as pd

import logging
log = logging.getLogger('UTILS')

def is_valid_uri(uri):
    """Determine if the string is a valid URL. We use it to distringuish links from workflow variables."""
    try:
        result = urllib.parse.urlparse(uri)
        return result.scheme and result.path
    except Exception as e:
        return False

def get_filename_from_uri(uri):
    """Get local file name given URI."""
    if uri.startswith('file://'):
        try:
            result = urllib.parse.urlparse(uri)
            return urllib.parse.ParseResult('', *result[1:]).geturl()
        except:
            return None
    elif uri.startswith('file:/'):
        return uri[len('file:/'):]
    elif uri.startswith('file:'):
        return uri[len('file:'):]
    else:
        return uri

def read_value_from_file(link):
    """Read Python object from the specified URI treated as a local file."""
    is_json = link.lower().endswith('json')
    is_pkl = link.lower().endswith('pkl')
    pathname = get_filename_from_uri(link)

    ret = None
    if is_pkl:
        if not os.path.exists(pathname):
            # Having no file means that a value represented by-reference is None
            return None
        try:
            with open(pathname, 'rb') as file:
                try:
                    ret = pickle.load(file)
                except Exception as e:
                    log.error("Error reading from file {0}. Exception: {1}".format(pathname, e))
                    return None
        except IOError as e:
            log.warning("Error reading from file {0}. Exception: {1}".format(pathname, e))
            return None
    elif is_json:
        return None
    else:
        return None

    return ret

def write_value_to_file(link, value):
    """Write Python object to the specified URI treated as a local file."""
    is_json = link.lower().endswith('json')
    is_pkl = link.lower().endswith('pkl')
    pathname = get_filename_from_uri(link)

    if is_pkl:
        try:
            with open(pathname, 'wb') as file:
                try:
                    pickle.dump(value, file)
                except Exception as e:
                    log.error("Error writing to file {0}. Exception: {1}".format(pathname, e))
                    return
        except IOError as e:
            log.warning("Error writing to file {0}. Exception: {1}".format(pathname, e))
            return
    elif is_json:
        pass
    else:
        pass

def get_columns(names, df=None):
    """Produce a list of column names by also validating them against the data frame."""
    result = []

    if isinstance(names, str):  # A single column name
        result.append(names)

    elif isinstance(names, (list, tuple)):  # A list of column names
        for col in names:
            if isinstance(col, str):
                result.append(col)
            elif isinstance(col, int):
                result.append(df.columns[col])
            else:
                log.error("Error reading column '{0}'. Names have to be strings or integers.".format(str(col)))
                return None

        # Process default (auto) values
        if len(result) == 0 and df is not None:  # Explicit empty list = ALL columns
            result = get_all_columns(df)

    elif isinstance(names, dict):  # An object specifying which columns to select
        exclude = names.get("exclude")
        if not isinstance(exclude, dict):
            exclude_columns = get_columns(exclude, df)
        else:
            log.error("Error reading column '{0}'. Excluded columns have to be (a list of) strings or integers.".format(exclude))
            return None

        # Get all columns and exclude the specified ones
        all_columns = get_all_columns(df)
        result = [x for x in all_columns if x not in exclude_columns]

    else:
        log.error("Column names have to be a list of strings or a string.")
        return None

    #
    # Validate
    #

    # Check that all columns are available
    if df is None:
        return result
    elif isinstance(df, pd.DataFrame):
        out = []
        for col in result:
            if col in df.columns:
                out.append(col)
            else:
                log.warning("Column '{0}' cannot be found. Skip column.".format(str(col)))
        return out

    elif isinstance(df, pd.core.groupby.groupby.DataFrameGroupBy):
        out = []
        for col in result:
            col_exists = False
            try:
                df.__getattr__(col)
                col_exists = True
            except:
                pass

            if col_exists:
                out.append(col)
            else:
                log.warning("Column '{0}' cannot be found. Skip column.".format(str(col)))
        return out

    return result

def get_all_columns(df):
    if df is None:
        return []
    elif isinstance(df, pd.DataFrame):
        return df.columns.tolist()
    elif isinstance(df, pd.core.groupby.groupby.DataFrameGroupBy):
        # TODO: We need to exclude key columns which are used for gropuing
        return df.obj.columns.tolist()
    else:
        return None

def all_columns_exist(names, df):
    all_columns_available = True
    for col in names:
        if col not in df.columns:
            all_columns_available = False
            log.warning("Column '{0}' is not available.".format(col))
            break
    if not all_columns_available:
        return False
    return True

def get_value(ref):
    """If the value is a reference then de-reference it and return. Otherwise, return this input value."""

    if not isinstance(ref, str):
        return ref

    if not ref.startswith('$'):
        return ref

    # Now we know that the value is a reference

    link = ref[1:]

    # De-reference depending on the link type
    value = None

    # Check if it is URL or workflow variable
    if is_valid_uri(link):
        is_file = link.lower().startswith('file:')

        if is_file:
            value = read_value_from_file(link)
        else:
            log.error("Reading values from URI is not implemented.")
            return None

    else:
        log.error("Reading values from workflow variables is not implemented.")
        return None

    return value

def set_value(ref, value):
    """If the value is a reference then write it to the referenced location."""

    if not isinstance(ref, str):
        return

    if not ref.startswith('$'):
        return

    # Now we know that the location is a reference

    link = ref[1:]

    # Check if it is URL or workflow variable
    if is_valid_uri(link):
        is_file = link.lower().startswith('file:')

        if is_file:
            write_value_to_file(link, value)
        else:
            log.error("Writing values to URI is not implemented.")
            return None

    else:
        log.error("Writing values to workflow variables is not implemented.")
        return None

def apply_row_filter(data, row_filter):
    """Select rows from the specified data frame which satisfy the provided row filter and return the result."""

    #
    # Column(s) with NaN as a predicate
    #
    drop_cols = row_filter.get("dropna")
    if isinstance(drop_cols, bool) and drop_cols is True:
        data.dropna(inplace=True)
    elif isinstance(drop_cols, (str, list)):
        cols = get_columns(drop_cols, data)
        data.dropna(subset=cols, inplace=True)
    elif drop_cols is not None:
        log.warning("Unknown dropna in row filter '{0}'. Dropna is either boolean or a list of columns.".format(drop_cols))

    #
    # Boolean column(s) as a predicate
    #
    predicate = row_filter.get("predicate")
    if predicate:
        pred_cols = get_columns(predicate, data)
        for col in pred_cols:
            pred_series = data[col]
            data = data[pred_series]  # Apply filter - only rows with true values will remain

        # By default, remove predicate columns because they are considered auxiliary and needed only for the purpose of removing rows
        data.drop(columns=pred_cols, inplace=True)

    #
    # Shuffle
    #
    sample = row_filter.get("sample", False)
    if sample:
        if isinstance(sample, bool):
            data = data.sample()
        elif isinstance(sample, dict):
            data = data.sample(**sample)
        else:
            log.warning("'sample' field '{0}' has to be boolean of dict. Ignored.".format(sample))
        data.reset_index(drop=True, inplace=True)

    #
    # Slice: row numbers as a predicate
    #
    slice = row_filter.get("slice")
    if slice:
        slice_start = slice.get("start", 0)
        slice_end = slice.get("end", len(data))
        slice_step = slice.get("step", 1)
        data = data.iloc[slice_start:slice_end:slice_step]

    # Ensure that tables always have 0-based index with continuous range
    data.reset_index(drop=True, inplace=True)

    return data


if __name__ == "__main__":
    pass
