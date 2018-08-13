__author__="Alexandr Savinov"

import json

from lambdo.utils import *
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

        #
        # Prepare execution environment
        #
        imports = self.workflow_json.get('imports', [])
        self.modules = import_modules(imports)

        #
        # Create table objects
        #
        self.tables = self.create_tables()

    def create_tables(self):
        """Create a list of Table objects from json."""
        tables_json = self.workflow_json.get("tables", [])
        tables = [Table(self,x) for x in tables_json]
        return tables

    def get_tables(self, table_names):
        """Find tables with the specified names"""
        if not table_names: return None
        tables = filter(lambda x: x.id in table_names, self.tables)
        return list(tables)

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
            this_table_no = self.workflow.get_table_number(self.id)
            if this_table_no and this_table_no > 0:
                input_table = self.workflow.tables[this_table_no-1]
                out = pd.DataFrame(input_table.data)
        elif len(tables) == 0:
            out = func(**model)
        elif len(tables) == 1:
            out = func(tables[0].data, **model)
        else:
            out = func(tables, **model)

        return out

    def execute(self):
        """
        Execute the whole table.
        This means populate it, execute all columns and then post-process.
        """
        log.info("===> Start populating table '{0}'".format(self.id))

        # Add records (populate)
        new_data = self.populate()
        if new_data is not None:
            self.data = new_data

        # Add derived columns (evaluate)
        for i, col in enumerate(self.columns):
            col.evaluate()

        #
        # Row filter
        #
        row_filter = self.table_json.get("row_filter")
        if row_filter:
            drop_cols = row_filter.get("dropna")
            if isinstance(drop_cols, bool) and drop_cols is True:
                self.data.dropna(inplace=True)
            elif isinstance(drop_cols, (str, list)):
                cols = get_columns(drop_cols, self.data)
                self.data.dropna(subset=cols, inplace=True)
            elif drop_cols is not None:
                log.warning("Unknown dropna in row filter '{0}'. Dropna is either boolean or a list of columns.".format(drop_cols))

            predicate = row_filter.get("predicate")
            if predicate:
                pred_cols = get_columns(predicate, self.data)
                for col in pred_cols:
                    pred_series = self.data[col]
                    self.data = self.data[pred_series] # Apply filter - only rows with true values will remain

                # By default, remove predicate columns because they are considered auxiliary and needed only for the purpose of removing rows
                self.data.drop(columns=pred_cols, inplace=True)

            self.data.reset_index(drop=True, inplace=True)  # Ensure that tables always have 0-based index with continuous range

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
        table = self.table.data  # Table the columns will be added to

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
            if not func:
                log.warning("Cannot resolve user-defined function '{0}'. Skip column definition.".format(func_name))
                break

            scope = definition.get('scope')

            #
            # Stage 4. Prepare input data argument to pass to the function (as the first argument)
            #
            data = table
            inputs = definition.get('inputs')
            if inputs is None:
                inputs = []
            inputs = get_columns(inputs, data)
            if inputs is None:
                log.warning("Error reading column list. Skip column definition.")
                break

            if inputs:
                all_inputs_available = True
                for col in inputs:
                    if col not in data.columns:
                        all_inputs_available = False
                        log.warning("Input column '{0}' is not available. Skip column definition.".format(col))
                        break
                if not all_inputs_available: break
                data = data[inputs]  # Select only specified columns
            else:
                pass  # All columns

            data_type = definition.get('data_type')

            #
            # Stage 5. Prepare model object to pass to the function (as the second argument)
            # It can be necessary to instantiate the argument object by using the specified class
            # It can be necessary to generate (train) a model (we need some specific logic to determine such a need)
            #
            model = definition.get('model')
            model_type = definition.get('model_type')
            train = definition.get('train')

            if not model and train is not None:

                # 1. Resolve train function
                train_func_name = train.get('function')
                train_func = resolve_full_name(train_func_name)
                if not train_func:
                    log.warning("Cannot resolve user-defined training function '{0}'. Skip training.".format(train_func_name))
                    break

                # 2. TODO: Determine input data

                # 3. Determine labels
                # - no labels at all (no argument is expected) - unsupervised learning
                # - explicitly specified outputs
                # - use output column specified in the transformation (but it has to be already available, e.g., loaded from source data, while the transformation will overwrite it)
                labels = train.get('outputs')
                if not labels:
                    labels = definition.get('outputs')  # Same columns as used by the transformation

                if labels:
                    labels = get_columns(labels, table)
                    if labels is None:
                        log.warning("Error reading column list. Skip column definition.")
                        break
                    y = table[labels]  # Select only specified columns
                else:
                    y = None  # Do not pass any labels at all

                # 4. Retrieve hyper-model
                train_model = train.get('model', {})

                # Cast data argument
                if data_type == 'ndarray':
                    data_arg = data.values
                    if y is not None:
                        y_arg = y.values
                else:
                    data_arg = data
                    if y is not None:
                        y_arg = y

                # 5. Make call and return model
                if y is None:
                    model = train_func(data_arg, **train_model)
                else:
                    if train_model is None:
                        model = train_func(data_arg, y_arg)
                    else:
                        model = train_func(data_arg, y_arg, **train_model)

            elif not model and not train:
                model = {}


            #
            # Stage 6. Apply function.
            # Depending on the "scope" the system will organize a loop over records, windows or make single call
            # It also depends on the call options (how and what to pass in data and model arguments, flatten json, ndarry or Series etc.)
            #

            out = transform(func, scope, data, data_type, model, model_type)

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

            # TODO: There result could be a complex object, while some option (like 'result_path') could provide a path to access it, so we need to be able to retrieve the result (either here or in transform function)
            # TODO: The result can be Series/listndarray(1d or 2d) and we need to convert it to DataFrame by using the original index.
            out = pd.DataFrame(out)  # Result can be ndarray
            for i, c in enumerate(out.columns):
                if outputs and i < len(outputs):  # Explicitly specified output column name
                    n = outputs[i]
                else:  # Same name - overwrite input column
                    n = inputs[i]
                table[n] = out[c]  # A column is attached by matching indexes so indexes have to be consistent (the same)

        #
        # Stage 8. Post-process the whole family
        #

        log.info("  <=== Finish evaluating column '{0}'".format(self.id))


if __name__ == "__main__":
    pass
