# Workflow definition

## Workflow structure

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

## Imports

Data processing in Lambdo relies on Python functions which do real data processing. Before these functions can be executed, they have to be imported. The location of functions to be imported is specified in a special field. For example, if the functions we want to use for data processing are in the `examples.example1` module and `datetime` module then we specify them as follows:

```json
{
  "id": "Example 1",
  "imports": ["examples.example1", "datetime"],
}
```

Now we can use functions from these modules in the workflow table and column definitions.
