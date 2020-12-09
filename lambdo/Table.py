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

        # Assign id
        self.id = self.table_json.get('id', None)
        if self.id is None:
            self.id = "___table___" + str(self.tableNo)
            self.table_json['id'] = self.id
            self.tableNo += 1

        # Create column objects
        columns_json = self.table_json.get("columns", [])
        self.columns = self._create_columns_from_descriptions()

    def _create_columns_from_descriptions(self):
        """
        Create a list of Column objects from json.
        Note that the list of json column definitions may include columns with extensions where each extension is used to create a column.
        """
        columns_json = self.table_json.get("columns", [])
        columns = []
        for family_col_json in columns_json:
            # One column definition may have extensions and hence we will get a list of concrete column definitions
            columns_json = build_json_extensions(family_col_json)

            for col_json in columns_json:
                col = Column(self, col_json)
                columns.append(col)

        return columns

    def create_column(self, definition):
        """
        Create a new column object from its description provided in the argument.
        The description is appended to the definition of the table (if it exists).
        """

        # Instantiate a column object and append it to the list of all columns
        column = Column(self, definition)
        self.columns.append(column)

        # TODO: Append only if such a column (with the same id) does not exist yet
        if self.table_json:
            if self.table_json.get("columns") is None:
                self.table_json["columns"] = []
            self.table_json.get("columns").append(definition)

        return column

    def get_column(self, column_name):
        """Find a column definition object with the specified name"""
        if not column_name: return None
        return next((x for x in self.columns if x.id == column_name), None)

    def get_column_number(self, column_name):
        """Find column definition number with this name"""
        return next((i for i, x in enumerate(self.columns) if x.id == column_name), -1)

    def get_columns(self, column_names):
        """Find column definitions with the specified names"""
        if not column_names: return None
        columns = filter(lambda x: x.id in column_names, self.columns)
        return list(columns)

    #
    # Data operations
    #

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
        operation = self.table_json.get('operation')
        if not operation:  # Default
            operation = 'all'

        if operation == 'all':
            new_data = self._populate_function()
            if new_data is not None:
                self.data = new_data
        else:
            log.warning("Unknown operation type '{0}' in definition of table {self.id}".format(operation))

        log.info("<=== Finish populating table '{0}'".format(self.id))

    def filter(self):
        """
        Apply filters for post-processing after the table and all its columns have been evaluated.
        It is a kind of a convenience approach where we define filters directly in the table without defining a new table.
        All other elements which depend on this table, will see the filtered result table.
        Therefore, in general, we need to apply filters before any element which depends on this table.
        """

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

    def _populate_function(self):
        """
        This operation type uses the provided UDF, model and inputs to fully populate this table.
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
