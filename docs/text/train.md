# Training a model

## Training a model from input data

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
