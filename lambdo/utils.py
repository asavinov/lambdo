__author__="Alexandr Savinov"

import logging
log = logging.getLogger('UTILS')


def get_columns(names, df=None):
    """Produce a list of column names by also validating them against the data frame."""
    result = []

    if isinstance(names, str):  # If a single name is provided (not a list)
        result.append(names)

    elif isinstance(names, (list, tuple)):
        for col in names:
            if isinstance(col, str):
                result.append(col)
                continue
            if isinstance(col, int):
                pass  # TODO: Retrieve column name with this number
            log.error("Error reading column name '{0}'. Names have to be strings.".format(str(col)))
            return None

        # Process default (auto) values
        if len(result) == 0 and df is not None:  # Explicit empty list = ALL columns
            result = df.columns.tolist()

    else:
        log.error("Column names have to be a list of strings or a string.")
        return None

    #
    # Validate
    #

    # Check that all columns are available
    if df is not None:
        for col in result:
            if col in df.columns:
                continue
            log.error("Column '{0}' does not exist in the table.".format(str(col)))
            return None

    return result
