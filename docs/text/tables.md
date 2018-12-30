# Table definition

## Table population function

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
