__author__="Alexandr Savinov"

import json

from lambdo.utils import *
from lambdo.resolve import *
from lambdo.transform import *

from lambdo.Workflow import *
from lambdo.Table import *
from lambdo.Column import *

import logging
log = logging.getLogger('TABLE')


class Table:
    """
    The class represents one table.
    A table is a collection of (its all input) columns.
    Table data is a set of tuples determined by key columns if any or by all its row ids.
    """

    tableNo = 0

    def __init__(self, workflow, table_json: dict):

        self.workflow = workflow
        self.table_json = table_json

        # TODO: Data represents the whole populated set and is a pandas index without columns (while columns are represented separately in columns using row ids as index)
        self.data = None

        self.id = self.table_json.get('id', None)
        if self.id is None:
            self.id = "___table___" + str(self.tableNo)
            self.table_json['id'] = self.id
            self.tableNo += 1

        # Create column objects
        columns_json = self.table_json.get("columns", [])
        self.columns = self.create_columns()

    def create_columns(self):
        """Create a list of Column objects from json."""
        columns_json = self.table_json.get("columns", [])
        columns = [Column(self,x) for x in columns_json]
        return columns

    def populate(self):
        """
        Populate this table by filling it with records.
        A record is a set element or tuple (complex values).
        The tuple is determined by its key columns or by row id if there are no keys.
        Population is determined by the table definition (operation) type: product, project, input/output etc.
        The result of population is a new pandas index with key columns stored in the data field.
        """
        log.info("===> Start populating table '{0}'".format(self.id))

        #
        # Apply an appropriate population function depending on the operation (definition) type. Currently on function-based definition
        #
        new_data = self.populate_function()
        if new_data is not None:
            self.data = new_data

        #
        # Add derived columns (evaluate)
        #
        for i, col in enumerate(self.columns):
            col.evaluate()

        #
        # Row filter
        #
        row_filter = self.table_json.get("row_filter")
        if row_filter:
            self.data = apply_row_filter(self.data, row_filter)

        #
        # Column filter
        #

        # Remove column which were marked for removal in their definition
        columns_exclude = []
        for i, col in enumerate(self.columns):
            ex = col.column_json.get("exclude")  # If a column definition has this flag then it will be removed
            if ex is True:
                columns_exclude.append(col.id)

        if columns_exclude:
            self.data.drop(columns=columns_exclude, inplace=True)

        # Remove columns from the list
        column_filter = self.table_json.get("column_filter")
        if column_filter:
            include_columns = get_columns(column_filter, self.data)
            if include_columns:
                self.data = self.data[include_columns]

        log.info("<=== Finish populating table '{0}'".format(self.id))

    def populate_function(self):
        """
        This operation type uses the provided UDF, model and inputs to populate this table.
        """
        #
        # Stage 1. Resolve the function
        #
        func_name = self.table_json.get('function')
        func = resolve_full_name(func_name)

        #
        # Stage 2. Prepare input data
        #
        inputs = self.table_json.get('inputs')
        tables = self.workflow.get_tables(inputs)
        if not tables: tables = []

        #
        # Stage 3. Prepare argument object to pass to the function as the second argument
        #
        model = self.table_json.get('model', {})

        #
        # Stage 6. Apply function
        #
        out = None
        if not func:
            if self.data is not None:  # Data is already available. Mainly for debug purposes: we set data programmatically and want to use it as it is
                out = self.data
            elif len(tables) == 0:  # No function, no input. Inherit data from the previous table in the workflow
                this_table_no = self.workflow.get_table_number(self.id)
                if this_table_no and this_table_no > 0:  # It is not the very first table in the workflow
                    input_table = self.workflow.tables[this_table_no-1]
                    out = pd.DataFrame(input_table.data)
                else:  # No previous table
                    out = None
        elif len(tables) == 0:
            out = func(**model)
        elif len(tables) == 1:
            out = func(tables[0].data, **model)
        else:
            out = func([t.data for t in tables], **model)

        return out


if __name__ == "__main__":
    pass
