# Databricks notebook source
# MAGIC %md # MLflow quickstart (Python)
# MAGIC 
# MAGIC With MLflow's autologging capabilities, a single line of code automatically logs the resulting model, the parameters used to create the model, and a model score. MLflow autologging is available for several widely used machine learning packages. This notebook creates a Random Forest model on a simple dataset and uses the the MLflow `autolog()` function to log information generated by the run.
# MAGIC 
# MAGIC For details about what information is logged with `autolog()`, refer to the [MLflow documentation](https://mlflow.org/docs/latest/index.html). 
# MAGIC 
# MAGIC ## Setup
# MAGIC * If you are using a cluster running Databricks Runtime, you must install the mlflow library from PyPI. See Cmd 3.
# MAGIC * If you are using a cluster running Databricks Runtime ML, the mlflow library is already installed. 

# COMMAND ----------

# MAGIC %md Install the mlflow library. 
# MAGIC This is required for Databricks Runtime clusters only. If you are using a cluster running Databricks Runtime ML, skip to Cmd 4. 

# COMMAND ----------

# If you are running Databricks Runtime version 7.1 or above, uncomment this line and run this cell:
#%pip install mlflow

# If you are running Databricks Runtime version 6.4 to 7.0, uncomment this line and run this cell:
#dbutils.library.installPyPI("mlflow")

# COMMAND ----------

# MAGIC %md Import the required libraries.

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import matplotlib.pyplot as plt

from numpy import savetxt

from sklearn.model_selection import train_test_split
from sklearn.datasets import load_diabetes

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

# COMMAND ----------

# MAGIC %md Import the dataset from scikit-learn and create the training and test datasets. 

# COMMAND ----------

db = load_diabetes()
X = db.data
y = db.target
X_train, X_test, y_train, y_test = train_test_split(X, y)

# COMMAND ----------

# MAGIC %md Create a random forest model and log parameters, metrics, and the model using `mlflow.sklearn.autolog()`.

# COMMAND ----------

# Enable autolog()
# mlflow.sklearn.autolog() requires mlflow 1.11.0 or above.
mlflow.sklearn.autolog()

# With autolog() enabled, all model parameters, a model score, and the fitted model are automatically logged.  
with mlflow.start_run():
  
  # Set the model parameters. 
  n_estimators = 100
  max_depth = 6
  max_features = 3
  
  # Create and train model.
  rf = RandomForestRegressor(n_estimators = n_estimators, max_depth = max_depth, max_features = max_features)
  rf.fit(X_train, y_train)
  
  # Use the model to make predictions on the test dataset.
  predictions = rf.predict(X_test)

# COMMAND ----------

predictions = rf.predict(X_test)
import pandas as pd
predictions_data=pd.DataFrame(predictions,columns=["Predictions"])
predictions_data.to_csv("predictions_data.csv")
import os
os.listdir()

# COMMAND ----------

# MAGIC %md To view the results, click **Experiment** at the upper right of this page. The Experiments sidebar appears. This sidebar displays the parameters and metrics for each run of this notebook. Click the circular arrows icon to refresh the display to include the latest runs. 
# MAGIC 
# MAGIC When you click the square icon with the arrow to the right of the date and time of the run, the Runs page opens in a new tab. This page shows all of the information that was logged from the run. Scroll down to the Artifacts section to find the logged model.
# MAGIC 
# MAGIC For more information, see View results ([AWS](https://docs.databricks.com/applications/mlflow/quick-start-python.html#view-results)|[Azure](https://docs.microsoft.com/azure/databricks/applications/mlflow/quick-start-python#view-results)|[GCP](https://docs.gcp.databricks.com/applications/mlflow/quick-start-python.html#view-results)).
