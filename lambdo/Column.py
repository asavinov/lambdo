__author__="Alexandr Savinov"

import json

from lambdo.utils import *
from lambdo.resolve import *
from lambdo.transform import *

from lambdo.Workflow import *
from lambdo.Table import *
from lambdo.Column import *

import logging
log = logging.getLogger('COLUMN')


class Column:
    """
    The class represents one column.
    It includes column input-output tables, column definition, its evaluation logic.
    A column definition may include extensions, which is a way to define families of columns with small differences in their definitiosn.
    There are several definition (operation) types each having its own parameters: calculate, accumulate, aggregate etc.
    """

    columnNo = 0

    def __init__(self, table, column_json: dict):

        self.table = table
        self.column_json = column_json

        # TODO: Data represents the whole function and is a pandas series with index as row ids of the table data
        self.data = None

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
        Evaluation logic depends on the operation (definition) kind.
        """
        log.info("  ---> Start evaluating column '{0}'".format(self.id))

        #
        # Stage 1: Ensure that the data field (with table data) is ready for applying column operations
        #
        table = self.table.data  # Table the columns will be added to

        #
        # Stage 2: Generate a list of concrete definitions by imposing extensions on the base definition
        # "extensions" field determine family or not.
        #
        concrete_definitions = self.get_definitions()
        num_extensions = len(concrete_definitions)

        # Essentially, we evaluate several columns independently
        for i, definition in enumerate(concrete_definitions):

            window = definition.get('window')

            operation = definition.get('operation')
            if not operation:  # Default
                if window is None or window == 'one' or window == '1':
                    operation = 'calculate'  # Default
                elif window == 'all':
                    operation = 'all'
                else:
                    operation = 'roll'

            #
            # Stage 3. Resolve the function
            #
            func_name = definition.get('function')
            if not func_name:
                log.warning("Column function is not specified. Skip column definition.".format(func_name))
                break

            func = resolve_full_name(func_name)
            if not func:
                log.warning("Cannot resolve user-defined function '{0}'. Skip column definition.".format(func_name))
                break

            #
            # Stage 4. Prepare input data argument to pass to the function (as the first argument)
            #
            data = table

            inputs = definition.get('inputs', [])
            inputs = get_columns(inputs, data)
            if inputs is None:
                log.warning("Error reading column list. Skip column definition.")
                break

            # Validation: check if all explicitly specified columns available
            if not all_columns_exist(inputs, data):
                log.warning("Not all columns available. Skip column definition.".format())
                break

            # Select only the specified input columns
            data = data[inputs]

            data_type = definition.get('data_type')

            #
            # Stage 5. Prepare model object to pass to the function (as the second argument)
            #
            model_type = definition.get('model_type')
            model = self.prepare_model(definition, inputs)
            if model is None:
                break

            #
            # Stage 6. Apply function.
            # Depending on the "window" the system will organize a loop over records, windows or make single call
            # It also depends on the call options (how and what to pass in data and model arguments, flatten json, ndarry or Series etc.)
            #
            out = transform(func, window, data, data_type, model, model_type)

            #
            # Stage 7. Post-process the result by renaming the output columns accordingly (some convention is needed to know what output to expect)
            #
            outputs = definition.get('outputs', [])
            if isinstance(outputs, str):  # If a single name is provided (not a list), then we wrap into a list
                outputs = [outputs]
            if not outputs:
                id = definition.get('id')
                # TODO: We could use a smarter logic here by finding a parameter of the extension which really changes (is overwritten): inputs, function, outputs, window, model etc.
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

        log.info("  <--- Finish evaluating column '{0}'".format(self.id))

    def prepare_model(self, definition, inputs):
        """
        Prepare model object to pass to the function (as the second argument)
        It can be necessary to instantiate the argument object by using the specified class
        It can be necessary to generate (train) a model (we need some specific logic to determine such a need)
        """

        model_ref = definition.get('model')
        model_type = definition.get('model_type')
        if model_ref and isinstance(model_ref, str) and model_ref.startswith('$'):
            log.info("Load model from {0}.".format(model_ref))
            model = get_value(
                model_ref)  # De-reference model which can be represented by-reference (if it is a string starting with $)
        else:
            model = model_ref

        train = definition.get('train')
        if model is None and train:

            model = self.train_model(definition, inputs)
            if model is None:
                return None

            # Each time a new model is generated, we store it in the model field of the definition
            if model and model_ref:
                log.info("Store trained model in {0}.".format(model_ref))
                set_value(model_ref, model)

        elif model is None and not train:
            model = {}

        return model

    def train_model(self, definition, inputs):

        train = definition.get('train')
        table = self.table.data

        data_type = definition.get('data_type')

        # 1. Resolve train function
        train_func_name = train.get('function')
        train_func = resolve_full_name(train_func_name)
        if not train_func:
            log.warning("Cannot resolve user-defined training function '{0}'. Skip training.".format(train_func_name))
            return None

        # 2. Filter rows for train data
        train_table = table
        train_row_filter = train.get("row_filter")
        if train_row_filter:
            train_table = apply_row_filter(table, train_row_filter)

        # 3. Select columns to use for training
        train_data = train_table
        train_inputs = train.get('inputs')
        if train_inputs is None:
            train_inputs = inputs  # Inherit from the 'apply' section
        train_inputs = get_columns(train_inputs, train_data)
        if train_inputs is None:
            log.warning("Error reading column list for training. Skip column training.")
            return None

        # Validation: check if all explicitly specified columns available
        if not all_columns_exist(train_inputs, train_data):
            log.warning("Not all columns available for training. Skip column definition.".format())
            return None

        # Select only specified columns
        train_data = train_data[train_inputs]

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
                return None
            train_labels = train_table[labels]  # Select only specified columns
        else:
            train_labels = None  # Do not pass any labels at all (unsupervised)

        # 4. Retrieve hyper-model
        train_model = train.get('model', {})

        # Cast data argument
        if data_type == 'ndarray':
            data_arg = train_data.values
            if train_labels is not None:
                labels_arg = train_labels.values
        else:
            data_arg = train_data
            if train_labels is not None:
                labels_arg = train_labels

        # 5. Call the function and generate a model
        if train_labels is None:
            model = train_func(data_arg, **train_model)
        else:
            if train_model is None:
                model = train_func(data_arg, labels_arg)
            else:
                model = train_func(data_arg, labels_arg, **train_model)

        return model


if __name__ == "__main__":
    pass
