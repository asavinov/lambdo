[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/conceptoriented/Lobby)
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://github.com/asavinov/lambdo/blob/master/LICENSE)
[![Python 3.6](https://img.shields.io/badge/python-3.6-brightgreen.svg)](https://www.python.org/downloads/release/python-360/)

# Feature engineering and machine learning: together at last!

Lambdo is a workflow engine which significantly simplifies the analysis process by *unifying* feature engineering and machine learning operations. Lambdo data analysis workflow does not distinguish between them and any node can be treated either as a feature or as prediction, and both of them can be trained.

Such a unification is possible because of the underlying *column-oriented* data processing paradigm which treats columns as first-class elements of the data processing pipeline having the same rights as tables. In Lambdo, a workflow consist of *table population* operations which process sets of records and *column evaluation* operations which produce new columns (features) from existing columns. This radically changes the way data is processed. The same approach is used also in Bistro: <https://github.com/asavinov/bistro>

Here are some unique distinguishing features of Lambdo:

* **No difference between features and models.** Lambdo unifies feature engineering and machine learning so that a workflow involves many feature definitions and many machine learning algorithms. It is especially important for deep learning where abstract intermediate features have to be learned.
* **One workflow for both prediction and training.** Lambdo nodes combine applying a transformation with training its model so that nodes of a workflow can be re-trained when required. This also guarantees that the same features will be used for both learning phase and prediction phase.
* **Columns first.**] Lambdo workflow use column operations along with table operations which makes many operations much simpler.
* **User-defined functions for extensibility.** Lambdo relies on user-defined functions which can be as simple as format conversion and as complex as deep learning networks.
* **Analysis of time-series and forecasting made easy.** Lambdo makes time series analysis much simpler by providing many using mechanisms like column families (for example, several moving averages with different window sizes), window-awareness (generation of windows is a built-in function), pre-defined functions for extracting goals.
* **As flexible as programming and as easy as IDE.** Lambdo is positioned between (Python) programming and interactive environments (like KNIME)

# Contents

* [Why Lambdo?](#why-lambdo)
  * [Why feature engineering?](#why-feature-engineering)
  * [Uniting feature engineering with data mining](#uniting-feature-engineering-with-data-mining)
  * [Any transformation model has an automatic training procedure](#any-transformation-model-has-an-automatic-training-procedure)
  * [Columns first: Column-orientation as a basis for feature engineering](#columns-first-column-orientation-as-a-basis-for-feature-engineering)
  * [Time series first: Time series analysis and forecasting made easy](#time-series-first-time-series-analysis-and-forecasting-made-easy)

* [Getting started with Lambdo](#getting-started-with-lambdo)
  * [Workflow definition](#workflow-definition)
    * [Workflow structure](#workflow-structure)
    * [Imports](#imports)
  * [Table definition](#table-definition)
    * [Table population function](#table-population-function)
    * [Column filter](#column-filter)
    * [Row filter](#row-filter)
  * [Column definition](#column-definition)
    * [Column evaluation function](#column-evaluation-function)
    * [Function scopes](#function-scopes)
    * [Training a model](#training-a-model)

* [Examples and analysis templates](#examples-and-analysis-templates)
  * [Example 1: Input and output](#example-1-input-and-output)
  * [Example 2: Record-based features](#example-2-record-based-features)
  * [Example 3: User-defined record-based features](#example-3-user-defined-record-based-features)
  * [Example 4: Table-based features](#example-4-table-based-features)

* [How to install](#how-to-install)
  * [Install from source code](#install-from-source-code)
  * [Install from package](#install-from-package)

* [How to test](#how-to-test)

* [How to use](#how-to-use)

# Why Lambdo?

## Why feature engineering?

In many cases, defining good features is more important than choosing and tuning a machine learning algorithm to be applied to the data. Hence, the quality of the data analysis result depends more on the quality of the generated features than on the machine learning model.

Such high importance of having good features is explained by the following factors:

*	It is a quite rare situation when you have enough data and even if you have it then it then probably you do not have enough computing resources to process it. In this situation, manually defined or automatically mined features compensate this lack of data or computing resources to process it. Essentially, we combine expert systems with data mining.

*	Feature engineering is a mechanism of creating new levels of abstraction in knowledge representation because each (non-trivial) feature extract and makes explicit some piece of knowledge hidden in the data. It is almost precisely what deep learning is intended for. In this sense, feature engineering does what hidden layers of a neural network do or what the convolutional layer of a neural network does.

## Uniting feature engineering with data mining

Let us assume that we want to compute moving average for a stock price. For each record, we compute an average value of this and some previous prices. This operation is interpreted as a transformation which generates a new feature stored as a column. Its result is defined by one parameter: window size (the number of days to be averaged including this day), and this number is essentially our transformation model.

Now let us assume that we want to add a second column which stores prices for tomorrow predicted using some algorithm from this and some previous values. We could develop a rather simple model which extrapolates price using previous values. This forecasting model, when applied, will also generate a new column with some values. Its result could depend on how many previous values are used by the extrapolation algorithm and this number is essentially our forecasting model.

An important observation here is that there is no difference between generating a new feature using some transformation model and generating a prediction using some forecast model. Technically, these are simply some transformations using some parameters, and these parameters are referred to as a model. Although there exist some exceptions where this analogy does not work, Lambdo assumes that it is so and follows the principle that

> Both generating a feature and applying a machine learning algorithm are data transformations parameterized by their specific models

Lambdo simply does not distinguish between them by assuming that any transformation that needs to be done is described by its (Python) function name and a (Python) model object. Lambdo will execute this function but it is unaware of its purpose. It can be a procedure for extracting dates from a string or it can be a prediction using a deep neural network model. In all these cases, the function will add new (derived) column to the existing table.

## Any transformation model has an automatic training procedure

One difference between feature generation and machine learning is that machine learning models cannot exist without an algorithm for their training - the whole idea of machine learning is that models are learned automatically from data rather than defined manually. On the hand, features can be defined either manually or learned from data. Since Lambdo is intended for unifying feature engineering and machine learning, it makes the following assumption:

> Any transformation has an accompanying function for generating its models

This means that first we define some transformation which is supposed to be applied to the data and produce a new feature or analysis result. However, the model for this transformation can be generated automatically (if necessary) before applying it. For example, even for computing moving averages an important question is what window size to choose, and instead of guessing we can delegate this question to the training procedure (which could use auto-regression to choose the best window size). Importantly, both procedures – applying a transformation and training its model – are part of the same workflow where they can be intermediate nodes and not only the final predicting node.

## Columns first: Column-orientation as a basis for feature engineering

Assume that we compute a moving average for a time series. The result of this operation is a new column. Now assume that we apply a clustering algorithm to records from a data set. In this case the result is again a new column. In fact, generating new columns is opposed to generating a new table and we can see these two operations in many data processing and data analysis approaches. Formally, we distinguish between set operations and operations with functions. Lambdo uses the following principle:

> Lambdo workflow consists of a graph of table definitions and each tables consists of a number of column definitions

A table definition describes how some set is populated using data in already existing tables. A typical table definition is a read/write operation or resampling time series or pivoting. A column definition describes how new values are evaluated using data in other columns in this and other tables.

This approach relies on the principles of the concept-oriented data model which also underlies [Bistro](https://github.com/asavinov/bistro) – an open source system for batch and stream data processing.

## Time series first: Time series analysis and forecasting made easy

Most existing machine learning algorithms are not time-aware and they cannot be directly applied to time series. Although Lambdo is a general purpose workflow engine which can be applied to any data, its functionality was developed with time series analysis and forecasting in mind. Therefore, it includes many mechanisms which make feature engineering much easier when working with time series:

*	Easily defining a family of columns which are features with only minor changes in their definitions. A typical but wide spread example is a family of features which use different window sizes.

*	Predefined column definitions for typical goal functions to be predicted or used for training intermediate features. Note that time series analysis is almost always supervised learning but there are different formulations for what we want to forecast.

*	Lambdo is window-aware workflow engine and for any transformation it is necessary to define its scope which means the number of rows the function will be applied to. This parameter is essentially the length of the history (number of previous records to be processed by the function).

*	Lambdo is going to be also object-aware which means it can partition the whole data set according to the value of the selected column interpreted as an object. This allows us to analyze data coming from multiple objects like devices, sensors, stock symbols etc.

*	Easy control of when to train which nodes. The problem here is that frequently a workflow has to be re-trained periodically but we do not want to re-train all nodes. This mechanism allows us to specify criteria for re-training its models.

# Getting started with Lambdo

Lambdo implements a novel *column-oriented* approach to data analysis by unifying various kinds of data processing: feature engineering, data transformations and data mining.

## Workflow definition

### Workflow structure

Lambdo is intended for processing data by using two types of transformations:

* Table transformations produce a new table given one or more input tables.
* Column transformations produce a new column in the table given one or more input columns.

A workflow is a number of table definitions each having a number of column definitions. These tables and columns compose a graph where edges are dependencies. If an element (table or column) in this graph has another element as its inputs then this dependency is an edge. If an element (table or column) does not have inputs then it is a data source. If an element does not have dependents then it is a data sink.

This data processing logic of Lambdo is represented in JSON format and stored in a workflow file and having the following structure: 

```javascript
{
  "tables": [
    "table": { "function": "my_table_func_1", "columns": [...] }
    "table": {
      "function": "my_table_func_2",
      "columns": {
        "column": { "function": "my_column_func_1", ... }
        "column": { "function": "my_column_func_2", ... }
      }
    "table": { "function": "my_table_func_3", "columns": [...] }
  ]
}
```

Each table and column definition has to specify a (Python) function name which will actually do data processing. Table definition will use functions for data population. Column definitions will use functions for evaluating new columns. When Lambo executes a workflow, it populates tables according to their definitions and evaluates columns (within tables) according to their definitions. Here it is important to understand that tables are used for set operations while columns are used for operations with mathematical functions.

### Imports

Data processing in Lambdo relies on Python functions which do real data processing. Before these functions can be executed, they have to be imported. The location of functions to be imported is specified in a special field. For example, if the functions we want to use for data processing are in the `examples.example1` module and `datetime` module then we specify them as follows:

```json
{
  "id": "Example 1",
  "imports": ["examples.example1", "datetime"],
}
```

Now we can use functions from these modules in the workflow table and column definitions.

## Table definition

### Table population function

A table definition has to provide some Python function which will *populate* this table. This function can be standard (built-in) Python function or it could be part of an imported module like `scale` function from the `sklearn.preprocessing` module or `BaseLibSVM.predict` function from the `sklearn.svm.base` module. Functions can be also defined for this specific workflow if they encode some domain-specific feature definition.

For example, if we want to read data then such a table could be defined as follows:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "inputs": [],
  "model": {
    "filepath_or_buffer": "my_file.csv",
    "nrows": 100
  }
}
```

Here we used a standard function from `pandas` but it could be any other function which returns a `DataFrame`.

Any function takes parameters which are referred to as a *model* and passed to the function. In the above example, we passed input file name and maximum umber of records to be read.

### Column filter

Frequently, it is necessary to generate some intermediate features (columns) which are not needed for the final analysis. Such features should be removed from the table. This can be done by specifying a *column filter* and this selection of necessary columns is performed always when all features within this table have been generated.

We can specify a list of columns, which have to be selected and passed to the next nodes in the graph:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "column_filter": ["Open", "Close"]
},
```

Alternatively, we can specify columns, which have to be excluded from the selected features to be passed to the next nodes:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "column_filter": {"exclude": ["Date"]}
},
```

The next table will then receive a table with all columns generated in this table excepting the `Date` column (which contains time stamps not needed for analysis).

### Row filter

Not all records in the table need to be analyzed and such records can be excluded before the table is passed to the next node for processing. Records to be removed are specified in the row filter which provides several methods for removal.

Many analysis algorithms cannot deal with `NaN` values and the simplest way to solve this problem is to remove all records which have at least one `NaN`:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "row_filter": {"dropna": true}
},
```

The `dropna` can also specify a list of columns and then only the values of these columns will be checked.

Another way to filter rows is to specify columns with boolean values and then the result table will retain only rows with `True` in these columns:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "row_filter": {"predicate": ["Selection"]}
},
```

A column with binary values can be defined precisely as any other derived column using a function, which knows which records are needed for analysis. (This column can be then removed by using a column filter.)

It is also possible to reandomly shuffle records by specifying the portion we want to keep in the table. This filter will keep only 80% of randomly selected records:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "row_filter": {"sample": {"frac": 0.8}
},
```

You can specify `"sample":true` if all records have to be shuffled.

The records can be also selected by specifying their integer position: start, end (exclusive) and step. The following filter will select every second record:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "row_filter": {"slice": {"start": 0, "step": 2}
},
```

## Column definition

### Column evaluation function

A column definition specifies how its values are computed from the values stored in other columns. The way these values are computed is implemented by some Python function which can be either a standard Python function, a function from some existing module or a user-defined function. Lambdo simply gets the name of this function from the workflow and then calls it to generate this column values.

A function is specified as a pair of its module and function name separated by a colon:

```javascript
"function": "my_module:my_function"
```

It is assumed that the first argument of the function is data to be processed and the second argument is a model which parameterizes this transformation. Note however that some function can take other parameters and also the type of these arguments can vary.

### Function scopes

What data a transformation function receives in its first argument? There are different options starting from a single value and ending with a whole input table. This is determined by the column definition parameter called `scope` which takes the following values:

* Scope `one` or `1` means that Lambdo will apply this function to every row of the input table and the function is expected to return a single value stored as the column value for this record. Type of data passed to the function depends on how many columns the `input` has.
  * If `input` has only one column then the function will receive a single value.
  * If `input` has more than 1 columns then the function will receive a `Series` object with their field values.
* Scope `all` means that the function will be applied to all rows of the table, that is, there will be one call and the whole table will be passed as a parameter. Type of the argument is `DataFrame`.
* Otherwise the system assume that the function has to be applied to all subsets of the table rows, called windows. Size of the window (number of records in one group) is scope value. For example, `scope: 5` means that each window will consists of 5 records. Type of this group depends on the number of columns in the `input`: 
  * If `input` has only one column then the function will receive a `Series` of values.
  * If `input` has more than 1 columns then the function will receive a `DataFrame` object with the records from the group.

### Training a model

A new feature is treated as a transformation, which results in a new column with the values derived from the data in other columns. This transformation is performed using some *model*, which is simply a set of parameters. A model can be specified explicitly by-value if we know these parameters. However, model parameters can be derived from the data using a separate procedure, called *training*. The transformation is then applied *after* the training.

How a model is trained is specified in a block within a column definition:

```json
{
  "id": "Prediction",
  "function": "examples.example1:gb_predict",
  "scope": "all",
  "inputs": {"exclude": ["Labels"]},
  "train": {
    "function": "examples.example1:gb_fit",
    "model": {"n_estimators": 500, "max_depth": 4, "min_samples_split": 2, "learning_rate": 0.01, "loss": "ls"},
    "outputs": ["Labels"]
  }
}
```

Here we need to specify a function which will perform such a training: `"function": "examples.example1:gb_fit"`. The training function also needs its own hyper-parameters, for example: `"model": {"max_depth": 4}`. Finally, the training procedure (in the case of supervised learning) needs labels: `"outputs": ["Labels"]`. Note also that excluded the `Labels` from the input so that they are not used as features for training.

Lambdo will first train a model by using the input data and then use this model for prediction.

# Examples and analysis templates

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
          "scope": "one",
          "inputs": "Date"
      }
    ]
}
```

The most important parameter in this column definition is `scope`. If it is `one` (or `1`) then the function will be applied to each row of the table. In other words, this function will get *one* row as its first argument. After evaluating this column definition, the table will get a new column `Datetime` storing time stamp objects (which are more convenient for further data transformations). 

If we do not need the source (string) column then the new column may get the same name `"id": "Date"` and it will overwrite the already existing column. Also, if the source column has a non-standard format then it can be specified in the model `"model": {"format": "%Y-%m-%d"}` which will be passed to the function.

```json
{
    "id": "Datetime",
    "function": "pandas.core.tools.datetimes:to_datetime",
    "scope": "one",
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

A record-based function with scope 1 will be applied to each row of the table and get this row fields in arguments. There will be as many calls as there are rows in the table. If `scope` is equal to `all` then the function will be called only one time and it will get all rows it has to process. Earlier, we described how string dates can be converted to datetime object by applying the transformation function to each row. The same result can be obtained if we pass the whole column to the transformation function. The only field that has to be changed in this definition is `scope`, which is now equals `all`:

```json
{
  "id": "Datetime",
  "function": "pandas.core.tools.datetimes:to_datetime",
  "scope": "all",
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
  "scope": "all",
  "inputs": ["Close"],
  "model": {"periods": -1}
}
```

Values of this new column will be equal to the value of the specified input column taken from the next record.

## Example 5: Window-based rolling aggregation

Lambdo is focused on time series analysis where important pieces of behavior (features) are hidden in sequences of events. Therefore, one of the main goals of feature engineering is making such features explicitly as attribute values by extracting data from the history. Normally it is done by using rolling aggregation where a function is applied to some historic window of recent events and returns one value, which characterizes the behavior. In Lambdo, it is possible to specify an arbitrary (Python) function, which encodes some domain-specific logic specific for this feature.

For window-based columns, the most important parameter is `scope` which is an integer specifying the number of events to be passed to the function as its first argument. For example, if `"scope": 10` then the Python function will always get 10 elements of the time series: this element and 9 previous elements. It can be a series of 10 values or a sub-table with 10 rows depending on other parameters. The function then analyzes these 10 events and returns one single value, which is stored as a value of this derived column.

Assume that we want to find running average volume for 10 days. This can be done as follows:

```json
{
  "id": "mean_Volume",
  "function": "numpy.core.fromnumeric:mean",
  "scope": 10,
  "inputs": ["Volume"]
}
```

Each value of the derived column `mean_Volume` will store average volume for the last 10 days. 

Note that instead of `mean` we could use arbitrary Python function including user-defined functions. Such a function will be called for each row in the table and it will get 10 values of the volume for the last 10 days (including this one). For example, we could write a function which counts the number of peaks (local maximums) in volume or we could find some more complex pattern. Also, if `inputs` has more columns then the functions will get a data frame as input with the columns specified in `inputs`.

Typically in time series analysis we use several running aggregations with different window sizes. Such columns can can be defined independently but their definitions will differ only in one parameter: `scope`. In order to simplify such definitions Lambdo allows for defining a base definition and extensions. For example, if we want to define average volumes with windows 10, 5 and 2 then this can be done by definition scopes in the extensions:

```json
{
  "id": "mean_Volume",
  "function": "numpy.core.fromnumeric:mean",
  "inputs": ["Volume"],
  "extensions": [
    {"scope": "10"},
    {"scope": "5"},
    {"scope": "2"}
  ]
}
```

The number of columns defined is equal to the number of extensions, that is, three columns in this examples. The names of the columns by default will be `id` of this family definition and the suffix `_N` where `N` is an index of the extension. In our example, three columns will be added after evaluating this definition: `mean_Volume_0`, `mean_Volume_1` and `mean_Volume_2`.

Moving averages can produce empty values, which we want to exclude from analysis, for example, because other analysis algorithms are not able to process them. Each table definition allows for filtering its records at the end before the table data is passed to the next node. In order to exclude all rows with empty values we add this block to the end of the table definition:

```json
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
```

This column definition does not have a model specified but it does specify a function for generating (training) such a model. It also provides a hyper-model for this training function which specifies how to do training. The training function gets the data and the hyper-model in its arguments and returns a trained model which is then used for generating the column data.

Here is an example of a column definition which trains and applies a gradient boosting data mining model:

```json
{
  "id": "Close_Tomorrow_Predict",
  "function": "examples.example6:gb_predict",
  "scope": "all",
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

```json
"model": "$file:my_model.pkl"
```

Now Lambdo will try to load this model from the file. If it succeeds then the model will be used for transformation (no training needed). If it fails, for example, the file does not exist, then Lambdo will generate this model by using the training function, store the model in the file and then use it for generating the column as usual.

Example 7 has one small modification with respect to Example 6: its trained model is stored in a file. As a result, we can apply this workflow to a large data set for training, and then this same workflow with the present model can be applied to smaller data sets for prediction.

## Example 8: Joining input tables

TBD

# How to install

## Install from source code

Check out the source code and execute this command in the project directory (where `setup.py` is located):

```console
$ pip install .
```

Or alternatively:

```console
$ python setup.py install
```

## Install from package

Create wheel package:

```console
$ python setup.py bdist_wheel
```

The `whl` package will be stored in the `dist` folder and can then be installed using `pip`.

Execute this command by specifying the `whl` file as a parameter:

```console
$ pip install dist\lambdo-0.1.0-py3-none-any.whl
```

# How to test

Run tests:

```console
$ python -m unittest discover -s ./tests
```

or

```console
$ python setup.py test
```

# How to use

If you execute `lambdo` without any options then it will return a short help by describing its usage.

A workflow file is needed to analyze data. Very simple workflows for test purposes can be found in the `tests` directory. More complex workflows can be found in the `examples` directory. To execute a workflow start `lambdo` with this workflow file name as a parameter:

```console
$ lambdo examples/example1.json
```

The workflow reads a CSV file, computes some features from the time series data, trains a model by applying it them to the data and finally writes the result to an output CSV file.
