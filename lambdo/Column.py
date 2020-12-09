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
    The class represents one column definition.
    A column definition should be distinguished from a data column because one definition can generate many data columns.
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

        # Assign id
        self.id = self.column_json.get('id', None)
        if self.id is None:
            self.id = "___column___" + str(self.columnNo)
            self.column_json['id'] = self.id
            self.columnNo += 1

    def _get_operation_type(self):
        """
        If operation type is specified explicitly then return it.
        Otherwise, determine the operation type from parameters.
        Each operation type uses its own set of parameters.
        Note that sometimes it is not possible to determine the operation type unambigously.
        """
        definition = self.column_json

        window = definition.get('window')

        operation = definition.get('operation')
        if not operation:  # If not specified explicitly then determine from parameters
            # Different operations use different parameters, particularly, window
            if window is None or window == 'one' or window == '1':
                operation = 'calculate'
            elif window == 'all':
                operation = 'all'
            else:
                operation = 'roll'
        return operation

    #
    # Data operations
    #

    def evaluate(self):
        """
        Evaluate this column.
        Evaluation logic depends on the operation (definition) kind.
        """
        log.info("---> Start evaluating column '{0}'".format(self.id))

        definition = self.column_json

        #
        # Determine operation type, that is, how the column will be generated
        #
        operation = self._get_operation_type()

        #
        # Stage 3. Resolve the function
        #
        func_name = definition.get('function')
        if not func_name:
            log.warning("Column function is not specified. Skip column definition.".format(func_name))
            return

        func = resolve_full_name(func_name)
        if not func:
            log.warning("Cannot resolve user-defined function '{0}'. Skip column definition.".format(func_name))
            return

        #
        # Stage 4. Prepare input data argument to pass to the function (as the first argument)
        #
        table = self.table.data  # Table the columns will be added to

        data = table

        inputs = definition.get('inputs', [])
        inputs = get_columns(inputs, data)
        if inputs is None:
            log.warning("Error reading column list. Skip column definition.")
            return

        # Validation: check if all explicitly specified columns available
        if not all_columns_exist(inputs, data):
            log.warning("Not all columns available. Skip column definition.".format())
            return

        # Select only the specified input columns
        data = data[inputs]

        data_type = definition.get('data_type')

        #
        # Stage 5. Prepare model object to pass to the function (as the second argument)
        #
        # Model is an arbitrary object.
        # It can be specified by-value as dict.
        # It can be a new instance of the specified class created using the specified constructor parameters.
        # Or it can be returned y the provided (training) procedure which
        # We pass inputs because 1) they are already prepared 2) we might need them for training
        model = self.prepare_model(definition, inputs)
        if model is None:
            return

        #
        # Stage 6. Evaluate column depending on the operation type
        #
        if operation == 'all':
            out = self._evaluate_all(func, data, data_type, model)
        elif operation == 'calculate':
            out = self._evaluate_calculate(func, data, data_type, model)
        elif operation == 'roll':
            out = self._evaluate_roll(func, data, data_type, model)
        else:
            log.warning("Unknown operation '{0}'. Skip column definition.".format(operation))
            return

        #
        # Stage 7. Post-process the result by renaming the output columns accordingly (some convention is needed to know what output to expect)
        #

        # 1)
        # Extensions: It is a convenience method of encoding (representing) definitions in JSON.
        # Once a definition is parsed, extensions do not exist anymore.
        # Thus extensions is a mechanism for simplifying writing definitions in JSON.

        # DEFINITION: A column object is a definition of an operation/procedure rather than one column. A column object can generate multiple columns.
        # If outputs are present, then they overwrite id.
        # In this sense, if outputs are used only for this purpose, then we should use only one field for naming result columns.
        # In any case, a column object does not represent a column in the table - its ids/outputs do.
        # Rather, a column object represents a *procedure*, which generates one or many columns.

        # A table has as many columns as it has outputs (in all column definitions)
        # A column data is actually an output, and we use column data names in other definitions
        # A column data name is a concatenation of its column definition name and its output name.
        # If outputs are not given, the column data name is equal to column definition name
        # If column definition name is not given, then each column data name is given by its outputs.

        # TODO: We need to define a logic of naming (single or multiple) physical columns depending on the names in "outputs".
        # TODO: This logic has to be used in establishing mappings between column object and its data objects.
        # We could assume that:
        # Naming:
        # - physical column names are provided explicitly via "outputs"
        # - if "outputs" are not given, then we use some convention like column name plus integer number
        # - Alternatively, we could assume that a real (physical) column name is a concatenation of its logical definition and "outputs" element
        # Resolving dependencies:
        # - Important: other (column) definitions use physical column names - not logical.
        # - More smart logic is to try to resolve a name using first physical column name, and then logical column name
        # - In any case, one definition can produce multiple output physical columns.
        # - Maybe currently do not implement this option, but we still need to take it into account.

        # In summary, a column definition may have extensions processed at compile time. The column id can be empty and is defined by extensions.
        # Each extension may produce multiple result columns at run time (retrieved from the output) and their names are specified as a list of id/outputs
        # We can assume that id IS an (output) column name, and if our function generates multiple

        # TODO: Output column extraction from an arbitrary (wrapper) object
        # Output can be one object and we can retrieve multiple columns using some paths/functions/methods.
        # Note that it is an essential feature, because some complex procedures generate several columns and we cannot solve this problem using wrappers.
        # If it a list-like array of results then we need to be able to convert it to individual Series to be added to the table.


        outputs = definition.get('outputs', [])
        if isinstance(outputs, str):  # If a single name is provided (not a list), then we wrap into a list
            outputs = [outputs]
        if not outputs:  # If outputs are not specified then use id
            outputs.append(definition.get('id'))

        # TODO: Extracting the necessary column (Series, ndarray etc.) from a complex object
        #  Acceess to the result is provided by specifying access path like 'result_path' or 'result_method'
        # For example, arima returns an object which contains forecast values and deviation values which have to be extracted and have different paths.
        # Note that works for only one output (if multiple then we need a list of result pahts/methods/functions)
        # Therefore, a list of paths/methods/functions can be provided along with a list of outputs/ids while the result is spposed to be one object.

        # TODO: The result can be Series/listndarray(1d or 2d) and we need to convert it to DataFrame by using the original index.
        out = pd.DataFrame(out)  # Result can be ndarray
        for i, c in enumerate(out.columns):
            if outputs and i < len(outputs):  # Explicitly specified output column name
                n = outputs[i]
            else:  # Same name - overwrite input column
                n = inputs[i]
            table[n] = out[c]  # A column is attached by matching indexes so indexes have to be consistent (the same)

        log.info("<--- Finish evaluating column '{0}'".format(self.id))

    def _evaluate_all(self, func, data, data_type, model):
        """
        All column definition. Apply function to all inputs and return its output(s).
        """

        #
        # Cast to the necessary argument type expected by the function
        #
        if data_type == 'ndarray':
            data_arg = data.values
            data_arg.reshape(-1, 1)
        elif (isinstance(data, pd.DataFrame) and len(data.columns) == 1):
            # data_arg = data
            data_arg = data[data.columns[0]]
        else:
            data_arg = data

        if isinstance(model, dict):
            out = func(data_arg, **model)  # Model as keyword arguments
        elif isinstance(model, (list, tuple)):
            out = func(data_arg, *model)  # Model as positional arguments
        else:
            out = func(data_arg, model)  # Model as an arbitrary object

        return out

    def _evaluate_calculate(self, func, data, data_type, model):
        """
        Calculate column definition. Apply function to each row of the table.
        """

        #
        # Single input: Apply to a series. UDF will get single value
        #
        if isinstance(data, pd.Series) or ((isinstance(data, pd.DataFrame) and len(data.columns) == 1)):
            if isinstance(data, pd.DataFrame):
                ser = data[data.columns[0]]
            else:
                ser = data

            #
            # Alternative ways to pass model: 1) flatten JSON 2) as one JSON argument 3) as one custom Python object
            #
            out = pd.Series.apply(ser, func, **model)  # Flatten model. One argument for each key of the model dictinary in UDF has to be declared.

        #
        # Multiple inputs: Apply to a frame. UDF will get a row of values
        #
        elif isinstance(data, pd.DataFrame):
            # Notes:
            # - UDF expects one row as a data input (raw=True - ndarry, raw=False - Series)
            # - model (arguments) cannot be None, so we need to guarantee that we do not pass None

            if data_type == 'ndarray':
                out = pd.DataFrame.apply(data, func, axis=1, raw=True, **model)
            else:
                out = pd.DataFrame.apply(data, func, axis=1, raw=False, **model)

        else:
            return None  # TODO: Either error or implement ndarray and other possible types

        return out

    def _evaluate_roll(self, func, data, data_type, model):
        """
        Roll column definition. Apply function to each window of the table.
        """

        #
        # Determine window size. The window parameter can be string, number or object (many arguments for rolling object)
        #
        definition = self.column_json
        window = definition.get('window')
        window_size = int(window)
        rolling_args = {'window': window_size}
        # TODO: try/catch with log message if cannot get window size

        #
        # Single input. Moving aggregation of one input column. Function will get a sub-series as a data argument
        #
        if len(data.columns) == 1:

            in_column = data.columns[0]

            # Create a rolling object with windowing (row-based windowing independent of the number of columns)
            by_window = pd.DataFrame.rolling(data, **rolling_args)  # as_index=False - aggregations will produce flat DataFrame instead of Series with index

            # Apply function to all windows
            if data_type == 'ndarray':
                out = by_window[in_column].apply(func, raw=True, **model)
            else:
                out = by_window[in_column].apply(func, raw=False, **model)

        #
        # Multiple inputs. Function will get a sub-dataframe as a data argument
        #
        else:

            #
            # Workaround: create a new temporary data frame with all row ids, create a rolling object by using it, apply UDF to it, the UDF will get a window/group of row ids which can be then used to access this window rows from the main data frame:
            # Link: https://stackoverflow.com/questions/45928761/rolling-pca-on-pandas-dataframe
            #

            df_idx = pd.DataFrame(np.arange(data.shape[0]))  # Temporary data frame with all row ids like 0,1,2,...
            idx_window = df_idx.rolling(**rolling_args)  # Create rolling object from ids-frame

            # Auxiliary function creates a subframe with data and passes it to the user function
            def window_fn(ids, user_f):
                return user_f(data.iloc[ids])

            out = idx_window.apply(lambda x: window_fn(x, func))

        return out

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
