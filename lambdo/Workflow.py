__author__="Alexandr Savinov"

import json

from lambdo.resolve import *
from lambdo.transform import *

import logging
log = logging.getLogger('WORKFLOW')


class Workflow:
    """
    The class represents a workflow.
    """

    workflowNo = 0

    def __init__(self, workflow_json: dict):

        self.workflow_json = workflow_json

        self.id = self.workflow_json.get('id', None)
        if self.id is None:
            self.id = "___workflow___" + str(self.workflowNo)
            self.workflow_json['id'] = self.id
            self.workflowNo += 1

        # Create table objects
        self.tables = self.create_tables()

    def create_tables(self):
        """Create a list of Table objects from json."""
        tables_json = self.workflow_json.get("tables", [])
        tables = [Table(self,x) for x in tables_json]
        return tables

    def get_tables(self, table_names):
        """Find tables with the specified names"""
        tables = filter(lambda x: x.id in table_names, self.tables)
        return tables

    def get_table_number(self, table_name):
        """Find table number in the list"""
        return next(i for i, x in enumerate(self.tables) if x.id == table_name)

    def execute(self):
        """
        Execute the whole workflow.
        This means executing all tables according to their dependencies.
        """
        log.info("Start executing workflow '{0}'.".format(self.id))

        # Execute all tables. Later we will take into account their dependencies.
        for i, tab in enumerate(self.tables):
            tab.execute()

        log.info("Finish executing workflow '{0}'.".format(self.id))


class Table:
    """
    The class represents one table.
    """

    tableNo = 0

    def __init__(self, workflow: Workflow, table_json: dict):

        self.workflow = workflow
        self.table_json = table_json
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
        Populate this table with records.
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

        #
        # Stage 3. Prepare argument object to pass to the function as the second argument
        #
        model = self.table_json.get('model', {})

        #
        # Stage 6. Apply function
        #
        out = None
        if not func:
            this_table_no = self.workflow.get_table_number(self.id)
            if this_table_no and this_table_no > 0:
                input_table = self.workflow.tables[this_table_no-1]
                out = pd.DataFrame(input_table.data)
        elif not inputs:
            out = func(**model)
        else:
            out = func(inputs, **model)

        return out

    def execute(self):
        """
        Execute the whole table.
        This means populate it, execute all columns and then post-process.
        """
        log.info("===> Start executing table '{0}'".format(self.id))

        # Add records (populate)
        new_data = self.populate()
        if new_data is not None:
            self.data = new_data

        # Add derived columns (evaluate)
        for i, col in enumerate(self.columns):
            col.evaluate()

        #
        # Table row filter
        #

        #
        # Table column filter
        #

        log.info("<=== Finish executing table '{0}'".format(self.id))

class Column:
    """
    The class represents one column definition.
    """

    columnNo = 0

    def __init__(self, table: Table, column_json: dict):

        self.table = table
        self.column_json = column_json

        self.id = self.column_json.get('id', None)
        if self.id is None:
            self.id = "___column___" + str(self.columnNo)
            self.column_json['id'] = self.id
            self.columnNo += 1

    def get_definitions(self):
        """
        Produce all concrete definitions by imposing extensions onto the base definition.
        :return: List of concrete definitions. In the case of no extensions, only the base definition is returned.
        """
        base = self.column_json.copy()
        exts = self.column_json.get('extensions')

        if not exts: return [base]  # No extensions

        result = []
        for ext in exts:
            e = {**base, **ext}
            e = dict(e)  # Make copy
            del e['extensions']  # Remove extensions
            result.append(e)

        return result

    def evaluate(self):
        """
        Evaluate this column.
        """
        log.info("  ===> Start evaluating column '{0}'".format(self.id))

        #
        # Stage 1: Ensure that "data" field is ready for applying column operations
        #

        #
        # Stage 2: Generate a list of concrete definitions by imposing extensions on the base definition
        # "extensions" field determine family or not.
        #
        concrete_definitions = self.get_definitions()
        num_extensions = len(concrete_definitions)

        for i, definition in enumerate(concrete_definitions):

            #
            # Stage 3. Resolve the function
            #
            func_name = definition.get('function')
            func = resolve_full_name(func_name)
            # TODO: Check errors (if func is None) and report in log or at least simply exit

            #
            # Stage 4. Prepare input data. Its rows (one, many or all) will be passed to the function as the second argument
            #
            X = self.table.data
            inputs = definition.get('inputs', [])
            if isinstance(inputs, str):  # If a single name is provided (not a list), then we wrap into a list
                inputs = [inputs]
            inX = None
            if inputs:
                inX = X[inputs]  # Select only specified columns
            else:
                inX = X  # All columns
            # TODO: One one input frame can be used (but previous operations in family can add new columns).
            # - detect 'data' field overwriting in extensions and report error
            # - resolve X from data field before the loop and use it in the loop body.

            #
            # Stage 5. Prepare argument object to pass to the function as the second argument
            # It might be necessary to instantiate the argument object by using the specified class
            #
            model = definition.get('model', {})

            #
            # Stage 6. Apply function.
            # Depending on the "scope" the system will organize a loop over records, windows or make single call
            # It also depends on the call options (how and what to pass in data and model arguments, flatten json, ndarry or Series etc.)
            #
            scope = definition.get('scope')
            options = definition.get('options')
            out = transform(func, inX, model, scope, options)

            #
            # Stage 7. Post-process the result by renaming the output columns accordingly (some convention is needed to know what output to expect)
            #
            outputs = definition.get('outputs', [])
            if isinstance(outputs, str):  # If a single name is provided (not a list), then we wrap into a list
                outputs = [outputs]
            if not outputs:
                id = definition.get('id')
                # TODO: We could use a smarter logic here by finding a parameter of the extension which really changes (is overwritten): inputs, function, outputs, scope, model etc.
                if num_extensions > 1:
                    id = id + '_' + str(i)
                outputs.append(id)

            res = pd.DataFrame(X)  # Copy all input columns to the result

            out = pd.DataFrame(out)  # Result can be ndarray
            for i, c in enumerate(out.columns):
                if outputs and i < len(outputs):  # Explicitly specified output column name
                    n = outputs[i]
                else:  # Same name - overwrite input column
                    n = inputs[i]
                res[n] = out[c]

        #
        # Stage 8. Post-process the whole family
        #

        log.info("  <=== Finish evaluating column '{0}'".format(self.id))


if __name__ == "__main__":
    pass
