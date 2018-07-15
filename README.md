# Lambdo: A workflow engine for time series feature engineering and forecasting

## Overview

Lambdo has the following characteristics:

* Lambdo is intended for time series feature engineering and forecasting
* Lambdo can work with both regular time series and asynchronous events
* Lambdo is inherently extensible by allowing for external and custom analysis algorithms and transformation techniques

Lambdo has the following distinguishing features:

* It heavily relies on feature engineering as the most important mechanism for (deep) data analysis
* New features are defined using user-defined functions (lambdas)
* A feature is treated as a model in the same sense and with the same status as data mining models
* Features can be trained and the produced models used for generating new features and predictions
* Training (feature learning) and predictions can be parts of the same workflow
* The workflows are designed to work for both training and prediction which is typically challange because of the necessity to have the same derived features
