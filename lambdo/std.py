__author__="Alexandr Savinov"

import os
import pickle
import urllib.parse

import pandas as pd

import logging
log = logging.getLogger('STD')

def join(dfs, **model):
    '''Use the first table as the main table and join all other tables using the specified model
    The model includes key to use for join (row numbers by default) and prefix/suffix used for column names added to the first table.
    '''

    # Data is a list of data frames. The first frame is a basis - all other frames will be attached to it
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
        main_df = pd.merge(main_df, df, how='left', left_on=left_on, right_on=right_on, left_index=left_index, right_index=right_index, suffixes=[main_suffix,right_suffix])

        # We always drop the second key columns because it stores identical data
        if left_on and right_on:
            main_df.drop(right_on, 1, inplace=True)

    # Return merged table
    return main_df

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
