# Principles of Lambdo

## Data analysis pipeline for feature engineering and machine learning tasks

### Two types of operations: Table population vs. column evaluation

A concept-oriented data model (COM) is defined as a number of *sets* and a number of *functions* between these sets. In contrast to purely set-oriented models, both sets and functions represent data. In particular, two different databases may well have the same sets but different functions.

Data transformations in COM are defined via *set operations* and *function operations*. A set operation (as usual) produces a new set given some input sets. A typical example of a set operation is join. A function operation produces a new function given some input functions. A typical example a function operation is calculating a new attribute given some other attributes. For example, this new attribute could be a sum of two existing attributes.

According to this principle:

> There are two types of operations in Lambdo: table transformations and column transformations

The task of the table transformation operation is to *populate* a new set by defining its elements. The task of the column transformation is to *evaluate* a new function by defining its outputs for element of the set.

Typically a workflow describing a data processing pipeline is described as a graph where nodes receive some input data tables, process this data, and produce an output data table. This output is then processed by other nodes in the graph. Lambdo uses a different approach. A Lambdo workflow consists of two types of nodes which can process data:

* Table definition
* Column definition

Accordingly, a Lambdo workflow is defined as a number of table definitions each having a number of column definitions. Such a workflow is represented in JSON has the following structure:

```javascript
{
  "tables": [
    "table": { "id": "Table 1", "columns": [
        { "id": "Column 1" },
        { "id": "Column 2" }
    ] },
    "table": { "id": "Table 1", "columns": [
        { "id": "Column 1" },
        { "id": "Column 2" }
    ] }
  ]
}
```

This workflow consists of two tables each having two columns. It does not however define how these tables are populated and how these columns are evaluated. Its purpose is to illustrate that any kind of data transformation and analysis in Lambdo is performed by means of these two types of operations. Note that it is a unique feature of Lambdo because other existing data processing models and systems rely on only set transformations.

### Between heaven and earth: Customization via user-defined functions

High level tools are intended for rapid prototyping and getting fast results by using intuitive visual interface where the user can create, connect and parameterize data transformation nodes. Yet, such tools frequently lack flexibility and lose control over what happens behind the scene at lower levels. As a result, either some data processing patterns are difficult or not possible to implement or the system lack other integration, customization functionality.

On the other hand, we can get almost full control over the process of data transformations by implementing all the necessary steps in some programming or scriping language like Python. Yet, it is a quite tedious and error-prone approach which requires high expertise.

Lambdo tries to provide means for working at both levels:

* at high level, Lambdo provides its workflow structure of table and column operations, and
* at low level, the nodes are customized using arbitrary user-defined (Python) functions

In other words, how concretely tables are populated and how concretely columns are evaluated is not defined by Lambdo - it is the task of the developer to specify the necessary behavior of table and column nodes:

> The data processing logic in a Lambdo workflow is modularized in column and table definitions implemented via (Python) user-defined functions while the workflow determines the structure of computations

Such a customization is performed by specifying a (Python) function for each node in a special field:

```javascript
"table": { "id": "Table 1", "function": "populate_func_1", "columns": [
    { "id": "Column 1", "function": "evaluate_func_1" },
    { "id": "Column 2", "function": "evaluate_func_2" }
] },
```

These functions of course have to obey some conventions for getting parameters and returning the result. During workflow execution, Lambdo will simply prepare the necessary parameters, call these functions and then store the result. Lambdo is unaware of what concretely happens with the data in these functions and here the developer has full control of the data transformations.

## Features and machine learning models

### Features are transformations producing new columns

An important observation is that

> features are columns and computing a new feature means adding a new column to some table

This means that defining a new feature during data processing is reduced to defining a new column. It is precisely what is well supported by Lambdo in contrast to other approaches where you still need to think in terms of new tables in a data processing pipeline.

### Predictions are transformations producing new columns

Another important observation is that

> applying a machine learning model during prediction phase means computing a new column

Indeed, assume that we have a classification model which distinguishes dogs (class 1) from cats (class 3). If we apply this model to a set of images then each image will essentially get an additional attribute storing the class (either 1 or 2).

Lambdo relies on this principle and simplifies its approach to data processing by using only the notion of column transformation while features or predictions are two possible interpretations. In other words, we cannot say whether some transformation is a feature or it is a prediction: it depends on our understanding of their role in the data analysis pipeline. Typically, simple transformations are treated as features while more complex computations with inference are treated as predictions. Also, initial and intermediate transformations tend to be treated as features while the final result is interpreted as a prediction.

### A transformation is parameterized using a model

A transformation with all parameters hard-coded is a relatively rare case (of simple transformations). Complex transformations are typically parameterized so that it is easier to adapt it to different tasks or input data sets. For example, if we want to find a difference of temperature from average temperature, then initially we do not know this average temperature or it can very. Therefore, we assume that it is a parameter of the transformation. Such a (column) transformation function will then have one argument with input data (temperature) and one argument with the parameter (average temperature). The function will return the difference between the first argument and the second argument. Importantly, the data argument is a variable while the parameter is always constant.

In the case of machine learning, transformation parameters are referred to as a model. For example, in order to classify some objects, we need to provide a model. This model could define a linear function or it could describe a separating hyperplane between two classes. In any case, it is a set of parameters which is used by the classification function in order to return a value (class) for each input object (data).

Lambo relies on this observation and assumes that

> Every node definition in Lambdo workflow has a field, called `model`, which stores parameters passed to the transformation function

For example, finding a deviation from some value is described as follows:

```javascript
{
    "id": "Column 1", 
    "function": "diff_fn",
    "model": { "average_temperature": 21 },
},
```

Here it is assumed that the function will get one input value in its first argument and the model in its second argument. This function then computes the difference and returns it. For example, if the input object has temperature 23 then it will return 2. What is important here is that we can always change the model without changing the code. Another important note is that this model can be as complex as neural networks. Of course, the model can be empty.

### Models (parameters of transformations) can be learnt from the data

Specifying constant models for transformations is a convenient feature which however does not dramatically change our approach to data processing. What is really important is that 

> transformation models can be produced automatically by analyzing the input data to be transformed before the transformation is applied

Essentially, it is one of the corner stones of machine learning: there exist algorithms which allow us to avoid manually specifying transformations by deriving them automatically from the data. What is specific to Lambdo is that we apply this principle to all kinds of transformations independent whether they are interpreted as features, machine learning tasks or anything else. Lambdo simply assumes that:

> model of transformation can be returned by a special procedure associated with the node and which knows how to derive it from the input data

For example, if average temperature is not known and depends on the data set then the right (machine learning) approach is to define a training function which will compute this model from the data, and only after that apply this model to the data to compute the difference:

```javascript
{
    "id": "Column 1", 
    "function": "diff_fn",
    "train": {
        "function": "learn_average_fn",
        "model": {}
    }
},
```

Here instead of specifying our model with average value explicitly, we provide a function which knows how to train this model by deriving the average value from the input data. This `learn_average_fn` training function will be executed before applying the transformation, and its result (trained model with average value) will be passed to the `diff_fn` transformation function as usual. The transformation function is actually unaware where its model comes from.

In this example, we defined a very simple learnable feature which can derive its transformation parameters from the data. However, the same approach is used for arbitrarily complex machine learning model. Note that the training function has its own hyper-model. In this example, this hyper-parameter could specify the way we compute the average value but if it were SVM it could specify, for instance, `gamma` parameters.

## Tables operations for data transformations

TBD
