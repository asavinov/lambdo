# Column definition

## Column evaluation function

A column definition specifies how its values are computed from the values stored in other columns. The way these values are computed is implemented by some Python function which can be either a standard Python function, a function from some existing module or a user-defined function. Lambdo simply gets the name of this function from the workflow and then calls it to generate this column values.

A function is specified as a pair of its module and function name separated by a colon:

```javascript
"function": "my_module:my_function"
```

It is assumed that the first argument of the function is data to be processed and the second argument is a model which parameterizes this transformation. Note however that some function can take other parameters and also the type of these arguments can vary.

## Function window

What data a transformation function receives in its first argument? There are different options starting from a single value and ending with a whole input table. This is determined by the column definition parameter called `window` which takes the following values:

* Window `one` or `1` means that Lambdo will apply this function to every row of the input table and the function is expected to return a single value stored as the column value for this record. Type of data passed to the function depends on how many columns the `input` has.
  * If `input` has only one column then the function will receive a single value.
  * If `input` has more than 1 columns then the function will receive a `Series` object with their field values.
* Window `all` means that the function will be applied to all rows of the table, that is, there will be one call and the whole table will be passed as a parameter. Type of the argument is `DataFrame`.
* Otherwise the system assume that the function has to be applied to all subsets of the table rows, called windows. Size of the window (number of records in one group) is window value. For example, `window: 5` means that each window will consists of 5 records. Type of this group depends on the number of columns in the `input`: 
  * If `input` has only one column then the function will receive a `Series` of values.
  * If `input` has more than 1 columns then the function will receive a `DataFrame` object with the records from the group.

## Training a model

A new feature is treated as a transformation, which results in a new column with the values derived from the data in other columns. This transformation is performed using some *model*, which is simply a set of parameters. A model can be specified explicitly by-value if we know these parameters. However, model parameters can be derived from the data using a separate procedure, called *training*. The transformation is then applied *after* the training.

How a model is trained is specified in a block within a column definition:

```json
{
  "id": "Prediction",
  "function": "examples.example1:gb_predict",
  "window": "all",
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
