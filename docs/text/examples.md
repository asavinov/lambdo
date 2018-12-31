# Examples

## Example 1: Input and output

Assume that the data is stored in a CSV file and we want to use this data to produce new features or for data analysis. Loading data is a table population operation which is defined in some table node of the workflow. How the table is populated depends on the `function` of this definition. In our example, we want to re-use a standard `pandas` for loading CSV files. Such a table node is defined as follows:

```json
{
  "id": "Source table",
  "function": "pandas:read_csv",
  "inputs": [],
  "model": {
    "filepath_or_buffer": "my_file.csv",
    "nrows": 100
  }
}
```

After executing this node, it will store the data from this file. We could also use any other function for loading or generating data. For example, it could a function which produces random data.

Data output can also be performed by using a standard `pandas` function:

```json
{
  "id": "Source table",
  "function": "pandas:DataFrame.to_csv",
  "inputs": "Source table",
  "model": {
    "path_or_buf": "my_output.csv",
    "index": false
  }
}
```

Note that the `inputs` fields points to the table which needs to be processed. The result of its execution will be a new CSV file.

Run this example from command line by executing:

```console
$ lambdo examples/example1.json
```

Another useful standard function for storing a table is `to_json` with a possible model like `{"path_or_buf": "my_file.json.gz", "orient"="records", "lines"=True, "compression"="gzip"}` (the file will be compressed). To read a JSON file into a table, use the function `read_json`.

## Example 2: Record-based features

The table definition where we load data has no column definitions. However, we can easily add them. A typical use case is where we want to change the format or data type of some columns. For example, if the source file has a text field with a time stamp then we might want to convert it the `datetime` object which is done by defining a new column:

```json
{
    "id": "Source table",
    "function": "pandas:read_csv",
    "inputs": [],
    "model": {
        "filepath_or_buffer": "my_file.csv"
    },
    "columns": [
      {
          "id": "Datetime",
          "function": "pandas.core.tools.datetimes:to_datetime",
          "window": "one",
          "inputs": "Date"
      }
    ]
}
```

The most important parameter in this column definition is `window`. If it is `one` (or `1`) then the function will be applied to each row of the table. In other words, this function will get *one* row as its first argument. After evaluating this column definition, the table will get a new column `Datetime` storing time stamp objects (which are more convenient for further data transformations). 

If we do not need the source (string) column then the new column may get the same name `"id": "Date"` and it will overwrite the already existing column. Also, if the source column has a non-standard format then it can be specified in the model `"model": {"format": "%Y-%m-%d"}` which will be passed to the function.

```json
{
    "id": "Datetime",
    "function": "pandas.core.tools.datetimes:to_datetime",
    "window": "one",
    "inputs": "Date",
    "model": {"format": "%Y-%m-%d"}
}
```

If some source or intermediate columns are not needed for later analysis then they can be excluded by adding a column filter to the table definition where we can specify columns to retain as a list like `"column_filter": ["Open", "High", "Low", "Close", "Volume"]` or to exclude like `"column_filter": {"exclude": ["Adj Close"]}`.

Execute this workflow as follows:

```console
$ lambdo examples/example2.json
```

## Example 3: User-defined record-based features

Let us now assume that we want to analyze the difference between high and low daily prices and hence we need to derive such a column from two input columns `High` and `Low`. There is no such a standard function and hence we need to define our own domain-specific function which will return the derived value given some input values. This user-defined function is defined in a Python source file:

```python
def diff_fn(X):
    """
    Difference between first and second fields of the input Series.
    """
    if len(X) < 2: return None
    if not X[0] or not X[1]: return None
    if pd.isna(X[0]) or pd.isna(X[1]): return None
    return X[0] - X[1]
```

This function will get a Series object for each row of the input table. For each pair of numbers it will return their difference.

In order for the workflow to load this function definition, we need to specify its location in the workflow:

```json
{
  "id": "Example 3",
  "imports": ["examples.example3"],
}
```

The column definition, which uses this function is defined as follows:

```json
{
  "id": "diff_high_low",
  "function": "examples.example3:diff_fn",
  "inputs": ["High", "Low"]
}
```

We specified two columns which have to be passed as parameters to the user-defined functions: `"inputs": ["High", "Low"]`. The same function could be also applied to other column where we want to find difference. This function will be called for each row of the table and its return values will be stored in the new column.

Each function including this one can accept additional arguments via its `model` (similar to how we passed data format in the previous example).

## Example 4: Table-based features

A record-based function with window 1 will be applied to each row of the table and get this row fields in arguments. There will be as many calls as there are rows in the table. If `window` is equal to `all` then the function will be called only one time and it will get all rows it has to process. Earlier, we described how string dates can be converted to datetime object by applying the transformation function to each row. The same result can be obtained if we pass the whole column to the transformation function. The only field that has to be changed in this definition is `window`, which is now equals `all`:

```json
{
  "id": "Datetime",
  "function": "pandas.core.tools.datetimes:to_datetime",
  "window": "all",
  "inputs": "Date",
  "model": {"format": "%Y-%m-%d"}
}
```

The result will be the same but this column will be evaluated faster.

Such functions which get all rows have to know how to iterate over the rows and they return one column rather than a single value. Such functions can apply any kind of computations because they have the whole data set. Therefore, such functions are used for more complex transformations including forecasts using some model.

Another example of applying a function to all rows is shifting a column. For example, if our goal is forecasting the closing price tomorrow then we need shift this column one step backwards:

```json
{
  "id": "Close_Tomorrow",
  "function": "pandas.core.series:Series.shift",
  "window": "all",
  "inputs": ["Close"],
  "model": {"periods": -1}
}
```

Values of this new column will be equal to the value of the specified input column taken from the next record.

## Example 5: Window-based rolling aggregation

Lambdo is focused on time series analysis where important pieces of behavior (features) are hidden in sequences of events. Therefore, one of the main goals of feature engineering is making such features explicitly as attribute values by extracting data from the history. Normally it is done by using rolling aggregation where a function is applied to some historic window of recent events and returns one value, which characterizes the behavior. In Lambdo, it is possible to specify an arbitrary (Python) function, which encodes some domain-specific logic specific for this feature.

For window-based columns, the most important parameter is `window` which is an integer specifying the number of events to be passed to the function as its first argument. For example, if `"window": 10` then the Python function will always get 10 elements of the time series: this element and 9 previous elements. It can be a series of 10 values or a sub-table with 10 rows depending on other parameters. The function then analyzes these 10 events and returns one single value, which is stored as a value of this derived column.

Assume that we want to find running average volume for 10 days. This can be done as follows:

```json
{
  "id": "mean_Volume",
  "function": "numpy.core.fromnumeric:mean",
  "window": 10,
  "inputs": ["Volume"]
}
```

Each value of the derived column `mean_Volume` will store average volume for the last 10 days. 

Note that instead of `mean` we could use arbitrary Python function including user-defined functions. Such a function will be called for each row in the table and it will get 10 values of the volume for the last 10 days (including this one). For example, we could write a function which counts the number of peaks (local maximums) in volume or we could find some more complex pattern. Also, if `inputs` has more columns then the functions will get a data frame as input with the columns specified in `inputs`.

Typically in time series analysis we use several running aggregations with different window sizes. Such columns can can be defined independently but their definitions will differ only in one parameter: `window`. In order to simplify such definitions Lambdo allows for defining a base definition and extensions. For example, if we want to define average volumes with windows 10, 5 and 2 then this can be done by defining windows in the extensions:

```json
{
  "id": "mean_Volume",
  "function": "numpy.core.fromnumeric:mean",
  "inputs": ["Volume"],
  "extensions": [
    {"window": "10"},
    {"window": "5"},
    {"window": "2"}
  ]
}
```

The number of columns defined is equal to the number of extensions, that is, three columns in this examples. The names of the columns by default will be `id` of this family definition and the suffix `_N` where `N` is an index of the extension. In our example, three columns will be added after evaluating this definition: `mean_Volume_0`, `mean_Volume_1` and `mean_Volume_2`.

Moving averages can produce empty values, which we want to exclude from analysis, for example, because other analysis algorithms are not able to process them. Each table definition allows for filtering its records at the end before the table data is passed to the next node. In order to exclude all rows with empty values we add this block to the end of the table definition:

```javascript
"row_filter": {"dropna": true}
```

Run this example and check out its result which will contain three new columns with moving averages of the volume:

```console
$ lambdo examples/example5.json
```

## Example 6: Training a model

All previous examples assumed that a column definition is treated as a data transformation performed via a Python function which also takes parameters of this transformation, which is called a model. The model describes how specifically the transformation has to be performed. One of the main features of Lambdo is that it treats such transformations as applying a data mining model. In other words, the result of applying a data mining model is a new column. For example, this column could store the cluster number this row belongs to or likelihood this object (row) is some object. What is specific to data mining is that its models are not specified explicitly but rather are trained from the data. This possibility to train a model (as opposed to providing an explicit model) is provided by Lambdo for any kind of column definition. If a model is absent and the training function is provided, then the model will be trained before it is applied to the data.

How a model has to be train is specified in a workflow using the `train` block of a column definition:

```json
{
  "columns": [
    {
      "id": "My Column",
      "function": "my_transform_function",

      "train": {
      "function": "my_train_function",
      "model": {"hyper_param": 123}
      }
    }
  ]
}
```

This column definition does not have a model specified but it does specify a function for generating (training) such a model. It also provides a hyper-model for this training function which specifies how to do training. The training function gets the data and the hyper-model in its arguments and returns a trained model which is then used for generating the column data.

Here is an example of a column definition which trains and applies a gradient boosting data mining model:

```json
{
  "id": "Close_Tomorrow_Predict",
  "function": "examples.example6:gb_predict",
  "window": "all",
  "data_type": "ndarray",
  "inputs": {"exclude": ["Date", "Close_Tomorrow"]},
  "train": {
    "function": "examples.example6:gb_fit",
    "row_filter": {"slice": {"end": 900}},
    "model": {"n_estimators": 500, "max_depth": 4, "min_samples_split": 2, "learning_rate": 0.01, "loss": "ls"},
    "outputs": ["Close_Tomorrow"]
  }
}
```

This definition has the following new elements:

* `data_type` field indicates that the functions accepts `ndarray` and not `DataFrame`.
* `inputs` field allows us to select columns we want to use. We want to exclude the `Date` column because its data type is not supported and `Close_Tomorrow` column which is a goal
* `row_filter` is used to limit the number of records for training
* `model` in the training section provides hyper-parameters for the training function
* `outputs` field specifies labels for the training procedure

Thus in this definition we want to use 900 records and all columns except for `Date` for training by using `Close_Tomorrow` as labels. The resulted model is then applied to *all* the data and the predictions are stored as the `Close_Tomorrow_Predict` column.

Run this example and check out its results in the last column with predictions:

```console
$ lambdo examples/example6.json
```

## Example 7: Reading and writing models

If a column (feature definition) model is not specified then Lambdo will try to generate it by using the train function. This trained model will be then applied to the input data. However, after finishing executing the workflow, this model will be lost. In many scenarios we would like to retain some trained models. In particular, it is necessary if we explicitly separate two phases: training and predicting. The goal of the training phase is to generate a prediction model by using some (typically large amount of) input data. The model resulted from the training phase can be then used for prediction (by this same workflow because we want to have the same features). Therefore, we do not want to train it again but rather load it from the location where it was stored during training.

The mechanism of storing and loading models is implemented by Lambdo as one of its main features. The idea is that workflow field values can specified either by-value or by-reference. Specifying a value by-value means that we simply provide a JSON object for the corresponding JSON field. However, we can also point to values by providing a reference which will be used by the system for reading or writing it.

The general rule is that if a JSON field value is a string which starts from `$` sign then it is a reference. If we want to store some model in a file (and not directly in the workflow) then the location is specified as follows:

```javascript
"model": "$file:my_model.pkl"
```

Now Lambdo will try to load this model from the file. If it succeeds then the model will be used for transformation (no training needed). If it fails, for example, the file does not exist, then Lambdo will generate this model by using the training function, store the model in the file and then use it for generating the column as usual.

Example 7 has one small modification with respect to Example 6: its trained model is stored in a file. As a result, we can apply this workflow to a large data set for training, and then this same workflow with the present model can be applied to smaller data sets for prediction.

## Example 8: Joining input tables

Frequently it is necessary to load data from many different data sources and merge them into one table the columns of which can be then used for generating features and analysis. Lambdo provides a standard table function `lambdo.std:join`  which populates a new table by joining data from a list of input tables. For example, assume that we want to analyze daily quotes for some symbol but in addition we want to load another quote data for the same days. The two input tables are specified in the `input` field. The first table `GSPC` in this list is treated as a main table while the second table `VIX` is attached to it:

```json
{
  "id": "Merged Table",
  "function": "lambdo.std:join",
  "inputs": ["GSPC", "VIX"],
  "model": {"suffixes": ["", "_VIX"]},
}
```

This new table will contain as many records as the first table contains but in addition to its columns it will have also columns from the second table. The model of the join function allows for specifying suffixes for columns.

The `join` function by default join using the row numbers. If it is necessary to join by using columns then they can be specified in the `keys` field of the table model as a list of column names.

Example 8 demonstrate how to load quotes from two different files and then predict closing price of one symbol taking into account the data for the second symbol.

## Example 9: Train and apply

In the previous examples, we trained models with the only purpose: generate new features. Therefore, the new (feature) models were applied to the *same* data that was used for training. It is a scenario of pure feature engineering where the main result is a new data set, which is supposed to be analyzed by some other data mining algorithm within some other framework (including a separate Lambdo workflow).

In this example, we show how we can generate features and also train a final data mining model, which is applied to previously unseen data. This workflow combines the steps for generating new features (possibly by training feature models), training a final data mining model, and applying this model to some portion of data which has not been used for training (but which was transformed using the same features). This workflow can be used for validating various scenarios or for tuning various parameters of the workflow.

The general goal is to predict the price. But simply using future (numeric) prices is a somewhat naive approach. We implement a more realistic scenario where the goal is to determine whether the price will exceed a threshold during some future interval (both the threshold and the length of the interval are parameters). For example, we might be interested to determine whether the price will be 2% higher (at least once) during next 10 days. Such a derived goal column has to store 1 if the price exceeds the threshold and 0 otherwise.

Deriving such a column is performed by performing the following steps:

* Find maximum price for the previous 10 days by applying the standard function `amax` to the column `High` with the window 10.
* Shift the column, which essentially means that now it represents the maximum future price.
* Find the relative increase of this future maximum price (in percent) by applying a user-defined function `rel_diff_fn`.
* Compare the relative increase with the given threshold and return either 1 or 0 by applying a user-defined function `ge_fn`.

After we computed the target column, we need to train a classification model using some portion of the input data set and then apply this model to the whole data set. According to the Lambdo conception, it is performed by defining a new column, which will store the result of classification but the model can be trained using the input data:

```javascript
{
  "id": "high_growth_lr",
  "function": "examples.example9:c_predict",
  "window": "all",
  "inputs": {"exclude": ["max_Price_future", "high_growth"]},
  "model": "$file:example9_model_lr.pkl",  // Read and use model from this file
  "train": {  // If file with model is not available then train the model

    "row_filter": {"slice": {"end": 6000}},  // Use only part of the data set for training

    "function": "examples.example9:lr_fit",
    "model": {},

    "outputs": ["high_growth"]  // Goal variable for training (labels)
  }
},
```

This column definition will try to load the model (of classification) from the specified file. If this file is present then no training will be done. If the file is not found then the model will be trained and the result will be stored in the file as well as used for classification. Note that the model is trained on only a subset of all input data because we defined a `row_filter`.

It is important to note that the training procedure uses the same features which will be used during forecast, that is, the previous parts of the workflow are reused. Another interesting option is that we actually can define several classification algorithms simultaneously the results of which can be then compared or even used as normal derived feature for further analysis.
