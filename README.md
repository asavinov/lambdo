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

## How it works

Data processing logic including data sources and data sinks is described in JSON format and stored in a file. At the highest level, Lambdo workflow is a number of table definitions each involving some number of column definitions:

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

Each table or column definition has to specify a (Python) function name which will actually do data processing. Table definition will use functions for data population. Column definitions will use functions for evaluating new columns. When Lambo executes a workflow, it populates tables according to their definitions and evaluates columns (within tables) according to their definitions. Here it is important to understand that tables are used for set operations while columns are used for operations with functions.

The function names specified in the workflow represent Python functions. They could be standard (built-in) Python functions or they could be part of an imported module like `scale` function from the `sklearn.preprocessing` module or `BaseLibSVM.predict` function from the `sklearn.svm.base` module. Functions can be also defined for this specific workflow if they encode some domain-specific feature definition.

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
