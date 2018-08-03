# Feature engineering and machine learning: together at last!

## What is Lambdo

Lambdo is a workflow engine which significantly simplifies the analysis process by *unifying* feature engineering and machine learning operations. Lambdo data analysis workflow does not distinguish between them and any node can be treated either as a feature or as prediction, and both of them can be trained.

Such a unification is possible because of the underlying *column-oriented* data processing paradigm which treats columns as first-class elements of the data processing pipeline having the same rights as tables. In Lambdo, a workflow consist of *table population* operations which process sets of records and *column evaluation* operations which produce new columns (features) from existing columns. This radically changes the way data is processed. The same approach is used also in Bistro: <https://github.com/asavinov/bistro>

Here are some unique distinguishing features of Lambdo:

* **No difference between features and models.** Lambdo unifies feature engineering and machine learning so that a workflow involves many feature definitions and many machine learning algorithms. It is especially important for deep learning where abstract intermediate features have to be learned.
* **One workflow for both prediction and training.** Lambdo nodes combine applying a transformation with training its model so that nodes of a workflow can be re-trained when required. This also guarantees that the same features will be used for both learning phase and prediction phase.
* **Columns first.**] Lambdo workflow use column operations along with table operations which makes many operations much simpler.
* **User-defined functions for extensibility.** Lambdo relies on user-defined functions which can be as simple as format conversion and as complex as deep learning networks.
* **Analysis of time-series and forecasting made easy.** Lambdo makes time series analysis much simpler by providing many using mechanisms like column families (for example, several moving averages with different window sizes), window-awareness (generation of windows is a built-in function), pre-defined functions for extracting goals.
* **As flexible as programming and as easy as IDE.** Lambdo is positioned between (Python) programming and interactive environments (like KNIME).

## Why Lambdo?

### Why feature engineering?

In many cases, defining good features is more important than choosing and tuning a machine learning algorithm to be applied to the data. Hence, the quality of the data analysis result depends more on the quality of the generated features than on the machine learning model.

Such high importance of having good features is explained by the following factors:

*	It is a quite rare situation when you have enough data and even if you have it then it then probably you do not have enough computing resources to process it. In this situation, manually defined or automatically mined features compensate this lack of data or computing resources to process it. Essentially, we combine expert systems with data mining.

*	Feature engineering is a mechanism of creating new levels of abstraction in knowledge representation because each (non-trivial) feature extract and makes explicit some piece of knowledge hidden in the data. It is almost precisely what deep learning is intended for. In this sense, feature engineering does what hidden layers of a neural network do or what the convolutional layer of a neural network does.

### Unite feature engineering with data mining

Let us assume that we want to compute moving average for a stock price. For each record, we compute an average value of this and some previous prices. This operation is interpreted as a transformation which generates a new feature stored as a column. Its result is defined by one parameter: window size (the number of days to be averaged including this day), and this number is essentially our transformation model.

Now let us assume that we want to add a second column which stores prices for tomorrow predicted using some algorithm from this and some previous values. We could develop a rather simple model which extrapolates price using previous values. This forecasting model, when applied, will also generate a new column with some values. Its result could depend on how many previous values are used by the extrapolation algorithm and this number is essentially our forecasting model.

An important observation here is that there is no difference between generating a new feature using some transformation model and generating a prediction using some forecast model. Technically, these are simply some transformations using some parameters, and these parameters are referred to as a model. Although there exist some exceptions where this analogy does not work, Lambdo assumes that it is so and follows the principle that

> Bosth generating a feature and applying a machine learning algorithm are data transformations parameterized by their specific models

Lambdo simply does not distinguish between them by assuming that any transformation that needs to be done is described by its (Python) function name and a (Python) model object. Lambdo will execute this function but it is unaware of its purpose. It can be a procedure for extracting dates from a string or it can be a prediction using a deep neural network model. In all these cases, the function will add new (derived) column to the existing table.

### Any transformation model has an automatic training procedure

One difference between feature generation and machine learning is that machine learning models cannot exist without an algorithm for their training - the whole idea of machine learning is that models are learned automatically from data rather than defined manually. On the hand, features can be defined either manually or learned from data. Since Lambdo is intended for unifying feature engineering and machine learning, it makes the following assumption:

> Any transformation has an accompanying function for generating its models

This means that first we define some transformation which is supposed to be applied to the data and produce a new feature or analysis result. However, the model for this transformation can be generated automatically (if necessary) before applying it. For example, even for computing moving averages an important question is what window size to choose, and instead of guessing we can delegate this question to the training procedure (which could use auto-regression to choose the best window size). Importantly, both procedures – applying a transformation and training its model – are part of the same workflow where they can be intermediate nodes and not only the final predicting node.

### Columns first: Column-orientation as a basis for feature engineering

Assume that we compute a moving average for a time series. The result of this operation is a new column. Now assume that we apply a clustering algorithm to records from a data set. In this case the result is again a new column. In fact, generating new columns is opposed to generating a new table and we can see these two operations in many data processing and data analysis approaches. Formally, we distinguish between set operations and operations with functions. Lambdo uses the following principle:

> Lambdo workflow consists of a graph of table definitions and each tables consists of a number of column definitions

A table definition describes how some set is populated using data in already existing tables. A typical table definition is a read/write operation or resampling time series or pivoting. A column definition describes how new values are evaluated using data in other columns in this and other tables.

This approach relies on the principles of the concept-oriented data model which also underlies [Bistro](https://github.com/asavinov/bistro) – an open source system for batch and stream data processing.

### Time series first: Time series analysis and forecasting made easy

Most existing machine learning algorithms are not time-aware and they cannot be directly applied to time series. Although Lambdo is a general purpose workflow engine which can be applied to any data, its functionality was developed with time series analysis and forecasting in mind. Therefore, it includes many mechanisms which make feature engineering much easier when working with time series:

*	Easily defining a family of columns which are features with only minor changes in their definitions. A typical but wide spread example is a family of features which use different window sizes.

*	Predefined column definitions for typical goal functions to be predicted or used for training intermediate features. Note that time series analysis is almost always supervised learning but there are different formulations for what we want to forecast.

*	Lambdo is window-aware workflow engine and for any transformation it is necessary to define its scope which means the number of rows the function will be applied to. This parameter is essentially the length of the history (number of previous records to be processed by the function).

*	Lambdo is going to be also object-aware which means it can partition the whole data set according to the value of the selected column interpreted as an object. This allows us to analyze data coming from multiple objects like devices, sensors, stock symbols etc.

*	Easy control of when to train which nodes. The problem here is that frequently a workflow has to be re-trained periodically but we do not want to re-train all nodes. This mechanism allows us to specify criteria for re-training its models.

## Getting started with Lambdo

Lambdo implements a novel column-oriented approach to data analysis with data transformations the models of which can be learned.

### Workflow

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

### Table definition

A table definition has to provide some Python function which will populate this table. This function can be standard (built-in) Python function or it could be part of an imported module like `scale` function from the `sklearn.preprocessing` module or `BaseLibSVM.predict` function from the `sklearn.svm.base` module. Functions can be also defined for this specific workflow if they encode some domain-specific feature definition.

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

### Column definition

#### Functions

A column definition specifies how its values are computed from the values stored in other columns. The way these values are computed is implemented by some Python function which can be either a standard Python function, a function from some existing module or a user-defined function. Lambdo simply gets the name of this function from the workflow and then calls it to generate this column values.

A function is specified as a pair of its module and function name separated by a colon:

```javascript
"function": "my_module:my_function"
```

It is assumed that the first argument of the function is data to be processed and the second argument is a model which parameterizes this transformation. Note however that some function can take other parameters and also the type of these arguments can vary.

#### Function scopes

What data a transformation function receives in its first argument? There are different options starting from a single value and ending with a whole input table. This is determined by the column definition parameter called `scope` which takes the following values:

* Scope `one` or `1` means that Lambdo will apply this function to every row of the input table and the function is expected to return a single value stored as the column value for this record. Type of data passed to the function depends on how many columns the `input` has.
  * If `input` has only one column then the function will receive a single value.
  * If `input` has more than 1 columns then the function will receive a `Series` object with their field values.
* Scope `all` means that the function will be applied to all rows of the table, that is, there will be one call and the whole table will be passed as a parameter. Type of the argument is `DataFrame`.
* Otherwise the system assume that the function has to be applied to all subsets of the table rows, called windows. Size of the window (number of records in one group) is scope value. For example, `scope: 5` means that each window will consists of 5 records. Type of this group depends on the number of columns in the `input`: 
  * If `input` has only one column then the function will receive a `Series` of values.
  * If `input` has more than 1 columns then the function will receive a `DataFrame` object with the records from the group.

## How to install

### Install from source code

Check out the source code and execute this command in the project directory (where `setup.py` is located):

```
$ pip install .
```

Or alternatively:

```
python setup.py install
```

### Install from package

Create wheel package:
```
$ python setup.py bdist_wheel
```
The `whl` package will be stored in the `dist` folder and can then be installed using `pip`.

Execute this command by specifying the `whl` file as a parameter:
```
pip install dist\lambdo-0.1.0-py3-none-any.whl
```

## How to test

Run tests:

```
$ python -m unittest discover -s ./tests
```

or

```
$ python setup.py test
```

## How to use

If you execute `lambdo` without any options then it will return a short help by describing its usage.

A workflow file is needed to analyze data. Very simple workflows for test purposes can be found in the `tests` directory. More complex workflows can be found in the `examples` directory. To execute a workflow start `lambdo` with this workflow file name as a parameter:

```
$ lambdo examples/example1.json
```

The workflow reads a CSV file, generates several moving averages for the volume column, and finally writes the result to an output CSV file.
