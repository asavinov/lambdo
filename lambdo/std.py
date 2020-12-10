__author__="Alexandr Savinov"

import os
import pickle
import urllib.parse

import pandas as pd

from lambdo.utils import *
from lambdo.resolve import *

import logging
log = logging.getLogger('STD')

def join(dfs, **model):
    """
    Use the first table as the main table and join all other tables using the specified model
    The model includes keys to use for join (row numbers by default) and prefix/suffix used for column names added to the first table.
    """

    # Data is a list of data frames. The first frame is a basis - all other frames will be linked to it
    main_df = dfs[0]
    secondary_dfs = dfs[1:]

    # Keys is a list of the same length as tables
    keys = model.get('keys', [])
    left_on = keys[0] if len(keys) > 0 else None
    left_index = not left_on
    right_ons = keys[1:]

    # Suffixes used for new column names
    suffixes = model.get('suffixes', [])
    main_suffix = suffixes[0] if len(suffixes) > 0 else None
    right_suffixes = suffixes[1:]

    # TODO: List of columns to retain (although not necessary because we can filter them before or after)

    #
    # Loop over all merged data frames
    #

    for i, df in enumerate(secondary_dfs):

        right_on = right_ons[i] if i < len(right_ons) else None
        right_index = not right_on

        right_suffix = right_suffixes[i] if i < len(right_suffixes) else None

        # Do join
        main_df = pd.merge(main_df, df, how='left', left_on=left_on, right_on=right_on, left_index=left_index, right_index=right_index, suffixes=[main_suffix,right_suffix], sort=False)

        # We always drop the second key columns because it stores identical data
        if left_on and right_on:
            main_df.drop(right_on, 1, inplace=True)

    # Return merged table
    return main_df

def aggregate(df, **model):
    """
    Group the records of the input table using the specified columns and then aggregate the groups into the column values of the output table.
    :param df:
    :param model:
    :return:
    """

    #
    # Read grouping column(s) and create a groupby object
    #
    by_columns = model.get('keys')
    by_columns = get_columns(by_columns, df)
    if not by_columns:
        log.error("Error in group keys in the table definition.")
        return

    try:
        gb = df.groupby(by_columns, sort=False, as_index=True)
    except Exception as e:
        log.error("Error grouping input table using the specified column(s).".format())
        log.debug(e)
        raise e

    #
    # Produce a new empty result table (the aggregate columns will be afterwards attached to)
    #
    out = gb.size()  # Produce a series with index as keys (which we need) and group sizes (which we do not need)
    out = pd.DataFrame(index=out.index)
    # Alternatively:
    # out = pd.DataFrame(list(gb.keys()), columns=by_columns)
    # out.set_index(grouping_columns, inplace=True)

    # Sort index if necessary
    # out.sort_index(inplace=True)

    #
    # TODO; For each aggregate column definition, add a new column to the output data frame
    #
    definitions_json = model.get('aggregations', [])

    for col_json in definitions_json:
        _aggregate_simple(gb, out, model, col_json)

    return out

def _aggregate_simple(gb, out, model, col_json):
    func_name = col_json.get('function')
    if not func_name:
        log.warning("Column function is not specified. Skip column definition.".format(func_name))
        return

    func = resolve_full_name(func_name)
    if not func:
        log.warning("Cannot resolve user-defined function '{0}'. Skip column definition.".format(func_name))
        return

    inputs = col_json.get('inputs', [])
    #inputs = get_columns(inputs, gb)

    if not inputs:  # Size of the group in records
        sr = gb.size()
    else:  # One column with data to be aggregated
        sr = gb[inputs].agg(func)  # udf will get a series with group values

    #
    # Determine output column name
    #
    outputs = col_json.get('outputs')
    if not outputs:
        outputs = col_json.get('id')

    #
    # Add the aggregated column to the result
    #
    out[outputs] = sr

def mean_weighted(df, **model):
    '''Find mean value of the first column weighted by the values in the second column.
    In the case all weights are equal, the result is mean value of the first column.
    '''
    if df is None or len(df) == 0:
        return None

    sum_weights = df.iloc[:,1].sum()

    # Use dot product of two columns
    sum_products = df.iloc[:,0].dot(df.iloc[:,1])

    # Alternative:
    #product = df.iloc[:,0] * df.iloc[:,1]
    #sum_products = product.sum()

    return sum_products / sum_weights


if __name__ == "__main__":
    pass
