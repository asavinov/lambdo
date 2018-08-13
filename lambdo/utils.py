__author__="Alexandr Savinov"

import logging
log = logging.getLogger('UTILS')


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
            result = df.columns.tolist()

    elif isinstance(names, dict):  # An object specifying which columns to select
        exclude = names.get("exclude")
        if not isinstance(exclude, dict):
            exclude_columns = get_columns(exclude, df)
        else:
            log.error("Error reading column '{0}'. Excluded columns have to be (a list of) strings or integers.".format(exclude))
            return None

        # Get all columns and exclude the specified ones
        all_columns = df.columns.tolist()
        result = [x for x in all_columns if x not in exclude_columns]

    else:
        log.error("Column names have to be a list of strings or a string.")
        return None

    #
    # Validate
    #

    # Check that all columns are available
    if df is not None:
        out = []
        for col in result:
            if col in df.columns:
                out.append(col)
            else:
                log.warning("Column '{0}' cannot be found. Skip column.".format(str(col)))
        return out

    return result
