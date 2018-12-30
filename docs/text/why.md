# Why Lambdo?

## Why feature engineering?

In many cases, defining good features is more important than choosing and tuning a machine learning algorithm to be applied to the data. Hence, the quality of the data analysis result depends more on the quality of the generated features than on the machine learning model.

Such high importance of having good features is explained by the following factors:

* It is a quite rare situation when you have enough data and even if you have it then it then probably you do not have enough computing resources to process it. In this situation, manually defined or automatically mined features compensate this lack of data or computing resources to process it. Essentially, we combine expert systems with data mining.

* Feature engineering is a mechanism of creating new levels of abstraction in knowledge representation because each (non-trivial) feature extract and makes explicit some piece of knowledge hidden in the data. It is almost precisely what deep learning is intended for. In this sense, feature engineering does what hidden layers of a neural network do or what the convolutional layer of a neural network does.

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

* Easily defining a family of columns which are features with only minor changes in their definitions. A typical but wide spread example is a family of features which use different window sizes.

* Predefined column definitions for typical goal functions to be predicted or used for training intermediate features. Note that time series analysis is almost always supervised learning but there are different formulations for what we want to forecast.

* Lambdo is window-aware workflow engine and for any transformation it is necessary to define its window which is the number of rows the function will be applied to. This parameter is essentially the length of the history (number of previous records to be processed by the function).

* Lambdo is going to be also object-aware which means it can partition the whole data set according to the value of the selected column interpreted as an object. This allows us to analyze data coming from multiple objects like devices, sensors, stock symbols etc.

* Easy control of when to train which nodes. The problem here is that frequently a workflow has to be re-trained periodically but we do not want to re-train all nodes. This mechanism allows us to specify criteria for re-training its models.
