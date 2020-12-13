__author__="Alexandr Savinov"

import json

from lambdo.utils import *
from lambdo.resolve import *
from lambdo.transform import *

from lambdo.Workflow import *
from lambdo.Table import *
from lambdo.Column import *

import logging
log = logging.getLogger('lambdo.table')


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

    def __repr__(self):
        return '['+self.id+']'

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

    def get_definitions_for_columns(self, column_names):
        """Find column definition objects which generate the column objects with the specified name(s)."""

        if isinstance(column_names, str):
            column_names = [column_names]
        column_names_set = set(column_names)

        ret = []
        for col in self.columns:  # For each existing definition, check if it generates at least one of the column names
            if set(col.get_outputs()) & column_names_set:
                ret.append(col)  # This definition generates some column specified in the argument

        return list(set(ret))

    def is_op_noop(self):
        operation = self.table_json.get('operation')
        if operation == 'noop':
            return True

        if operation:
            return False

        function = self.table_json.get('function')
        this_table_no = self.workflow.get_table_number(self.id)
        # No function specified and no parent table
        if not function and this_table_no == 0:
            return True
        return False

    def is_op_extend(self):
        operation = self.table_json.get('operation')
        if operation == 'extend':
            return True

        if operation:
            return False

        function = self.table_json.get('function')
        this_table_no = self.workflow.get_table_number(self.id)
        # No function specified and there exists previous table
        if not function and this_table_no > 0:
            return True
        return False

    def is_op_all(self):
        operation = self.table_json.get('operation')
        if operation == 'all':
            return True

        if operation:
            return False

        function = self.table_json.get('function')
        if function:
            return True
        return False

    def is_op_project(self):
        operation = self.table_json.get('operation')
        if operation == 'proj' or operation == 'project' or operation == 'projection':
            return True
        return False

    def is_op_product(self):
        operation = self.table_json.get('operation')
        if operation == 'prod' or operation == 'product':
            return True
        return False

    def is_op_join(self):
        operation = self.table_json.get('operation')
        if operation == 'join':
            return True

        if operation:
            return False

        function = self.table_json.get('function')
        if function and function == 'lambdo.std:join':
            return True
        return False

    def is_op_aggregate(self):
        operation = self.table_json.get('operation')
        if operation == 'aggregate':
            return True

        if operation:
            return False

        function = self.table_json.get('function')
        if function and function == 'lambdo.std:aggregate':
            return True
        return False

    def get_all_own_dependencies(self):
        """
        Return a list of definitions within this table which have to be executed in order to consider this table completed.
        This list includes this table (which means all its attributes), all derived columns, row filters, column filters etc.
        This method is used from other definitions which must work only with a completely generated tables.
        """
        dependencies = []

        dependencies.append(self)
        dependencies.extend(self.columns)

        # Add filter operation (Table, 'filter') if it has been defined
        if self.has_filters():
            dependencies.append((self, 'filter'))

        return dependencies

    def get_dependencies(self):
        """
        Get tables and columns this table depends upon.
        The returned elements must be executed before this element can be executed because this element consumes their data.
        """

        definition = self.table_json
        dependencies = []

        if self.is_op_noop():
            pass

        elif self.is_op_extend():
            # An extended table depends on its base table (which has to be fully generated)
            this_table_no = self.workflow.get_table_number(self.id)
            parent_table = self.workflow.tables[this_table_no - 1]
            dependencies.extend(parent_table.get_all_own_dependencies())  # We want to have all columns of the base table to be evaluated before extending it

        elif self.is_op_join():
            # A join table depends on its source tables
            inputs = definition.get('inputs', [])
            source_tables = self.workflow.get_tables(inputs)

            # We assume that the source tables have to be completely generated and hence include ALL their dependencies
            for source_table in source_tables:
                dependencies.extend(source_table.get_all_own_dependencies())

        elif self.is_op_aggregate():
            # TODO: An aggregate table depends on its source (fact) table and its measures and its link/grouping column.
            pass

        elif self.is_op_project():
            # A project table depends on its source table(s)
            source_table_name = definition.get('source_table')
            source_table = self.workflow.get_table(source_table_name)
            dependencies.append(source_table)

            # The source table key columns
            inputs = definition.get('inputs', [])
            dependencies.extend(source_table.get_definitions_for_columns(inputs))

        elif self.is_op_product():
            # TODO: A product table (including project tables and filter table) depends on its domain tables which have to be populated before because their ids will be used in this table attributes.
            pass

        elif self.is_op_all():
            # A function (all) table depends on the table declared in inputs
            inputs = definition.get('inputs', [])
            input_tables = self.workflow.get_tables(inputs)
            dependencies.extend(input_tables)

            for t in input_tables:
                # We want to have all columns of the input table to be evaluated before the tables are used (example: writing previous table to a file)
                dependencies.extend(t.get_all_own_dependencies())

        else:
            # TODO: Error: unknown operation type
            pass

        return dependencies

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
        operation = self.table_json.get('operation')
        log.info("===> Start populating table '{0}'. Operation '{1}'.".format(self.id, operation))

        #
        # Apply an appropriate population function depending on the operation (definition) type. Currently on function-based definition
        #
        if self.is_op_noop():
            new_data = None

        elif self.is_op_extend():
            new_data = self._populate_extend()

        elif self.is_op_join():
            new_data = self._populate_join()

        elif self.is_op_aggregate():
            new_data = self._populate_aggregate()

        elif self.is_op_project():
            new_data = self._populate_project()

        elif self.is_op_product():
            log.error("Product operation is not implemented")

        elif self.is_op_all():
            new_data = self._populate_function()

        else:
            log.warning("Unknown operation type '{0}' in the definition of table {1}".format(operation, self.id))

        if new_data is not None:
            self.data = new_data

        log.info("<=== Finish populating table '{0}'".format(self.id))

    def has_filters(self):
        row_filter = self.table_json.get("row_filter")
        if row_filter:
            return True

        column_filter = self.table_json.get("column_filter")
        if column_filter:
            return True

        for col in self.columns:
            if col.column_json.get("exclude"):
                return True

        return False

    def execute_filter(self):
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
        # Remove column which were marked for removal in their definition
        #
        columns_exclude = []
        for i, col in enumerate(self.columns):
            ex = col.column_json.get("exclude")  # If a column definition has this flag then it will be removed
            if ex is True:
                columns_exclude.append(col.id)

        if columns_exclude:
            self.data.drop(columns=columns_exclude, inplace=True)

        #
        # Column filter
        #
        column_filter = self.table_json.get("column_filter")
        if column_filter:
            include_columns = get_columns(column_filter, self.data)
            if include_columns:
                self.data = self.data[include_columns]

    def _populate_extend(self):
        """
        The base (typically previous) table is used to add columns in this table. No new records will be added.
        """
        definition = self.table_json
        this_table_no = self.workflow.get_table_number(self.id)
        base_table = self.workflow.tables[this_table_no-1]

        out = pd.DataFrame(base_table.data)

        return out

    def _populate_join(self):
        """
        Join input tables on the specified columns.
        """
        definition = self.table_json
        return self._populate_function()

    def _populate_aggregate(self):
        """
        Aggregate facts from input tables.
        """
        definition = self.table_json
        return self._populate_function()

    def _populate_function(self):
        """
        The specified UDF with the model and inputs will return a fully populated table.
        """
        definition = self.table_json

        #
        # Stage 1. Resolve the function
        #
        func_name = definition.get('function')
        func = resolve_full_name(func_name)

        #
        # Stage 2. Prepare input data
        #
        inputs = definition.get('inputs')
        tables = self.workflow.get_tables(inputs)
        if not tables: tables = []

        #
        # Stage 3. Prepare argument object to pass to the function as the second argument
        #
        model = definition.get('model', {})

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

    def _populate_project(self):
        """
        Data from the source table is projected along the specified source columns by using only unique combinations.
        """
        definition = self.table_json

        #
        # Read parameters
        #
        source_table_name = definition.get('source_table')
        source_table = self.workflow.get_table(source_table_name)
        if not source_table:
            log.error("Source table '{0}' cannot be found in the project column definition..".format(source_table_name))
            return

        inputs = definition.get('inputs')
        if not all_columns_exist(inputs, source_table.data):
            log.error("Not all source columns available in the project column definition.".format())
            return

        outputs = definition.get('outputs')

        #
        # Produce all unique combinations of the input columns
        #
        """
        INFO:
        df_new = df.drop_duplicates(subset=['C1', 'C2', 'C3'])  # Drop duplicates
        a_df = df.drop_duplicates(['col1', 'col2'])[['col1', 'col2']]
        df = df.groupby(by=['C1', 'C2', 'C3'], as_index=False).first()  # Using groupby
        np.unique(df[['col1', 'col2']], axis=0)  # Not for object data (error for object types)
        """
        out = source_table.data.drop_duplicates(subset=inputs)  # Really do projection
        out = out[inputs]  # Leave only project columns (de-duplicate will return all source columns)

        out.reset_index(drop=True, inplace=True)

        # Rename to output names
        if outputs:
            if len(outputs) != len(inputs):
                log.error("Number of output columns in the project column definition has to be equal to the number of input columns.".format())
                return

            rename_dict = dict(zip(inputs, outputs))
            out.rename(columns=rename_dict, inplace=True)

        return out


if __name__ == "__main__":
    pass
