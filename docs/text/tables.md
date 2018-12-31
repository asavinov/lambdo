# Table definition

## Table population

A table definition has to provide some Python function which will *populate* this table. This function can be standard (built-in) Python function or it could be part of an imported module like `scale` function from the `sklearn.preprocessing` module or `BaseLibSVM.predict` function from the `sklearn.svm.base` module. Functions can be also defined for this specific workflow if they encode some domain-specific feature definition.

Any function takes parameters which are referred to as a *model* and is specified in the `model` field of the table definition as a JSON object.

## Input and output data

Assume that the data is stored in a CSV file and we want to use this data to produce new features or for data analysis. Loading data from an external data source is a table population operation which is defined in some (source) table node of the workflow. How a table is populated depends on the `function` of this definition. In our example, we want to re-use a standard `pandas` for loading CSV files but it could be any other function which returns a `DataFrame`. Such a table node is defined as follows:

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

A model in this example specifies input file name and maximum number of records to be read. After executing this node, it will store the data from this file as a data frame. We could also use any other function for loading or generating data. For example, it could a function which produces random data or some intervals of dates.

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

## Joining input tables

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

## Column filter

Frequently, it is necessary to generate some intermediate features (columns) which are not needed for the final analysis. Such features should be removed from the table. This can be done by specifying a *column filter* and this selection of necessary columns is performed always when all features within this table have been generated.

We can specify a list of columns, which have to be selected and passed to the next nodes in the graph:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "column_filter": ["Open", "Close"]
}
```

Alternatively, we can specify columns, which have to be excluded from the selected features to be passed to the next nodes:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "column_filter": {"exclude": ["Date"]}
}
```

The next table will then receive a table with all columns generated in this table excepting the `Date` column (which contains time stamps not needed for analysis).

## Row filter

Not all records in the table need to be analyzed and such records can be excluded before the table is passed to the next node for processing. Records to be removed are specified in the row filter which provides several methods for removal.

Many analysis algorithms cannot deal with `NaN` values and the simplest way to solve this problem is to remove all records which have at least one `NaN`:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "row_filter": {"dropna": true}
}
```

The `dropna` can also specify a list of columns and then only the values of these columns will be checked.

Another way to filter rows is to specify columns with boolean values and then the result table will retain only rows with `True` in these columns:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "row_filter": {"predicate": ["Selection"]}
}
```

A column with binary values can be defined precisely as any other derived column using a function, which knows which records are needed for analysis. (This column can be then removed by using a column filter.)

It is also possible to reandomly shuffle records by specifying the portion we want to keep in the table. This filter will keep only 80% of randomly selected records:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "row_filter": {"sample": {"frac": 0.8}
}
```

You can specify `"sample":true` if all records have to be shuffled.

The records can be also selected by specifying their integer position: start, end (exclusive) and step. The following filter will select every second record:

```json
{
  "id": "My table",
  "function": "pandas:read_csv",
  "row_filter": {"slice": {"start": 0, "step": 2}
}
```
