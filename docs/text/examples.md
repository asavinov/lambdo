# Examples

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
