# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Datalake using cluster scoped credentials
# MAGIC 1. set the spark config to fs.azure.account.key in the cluster
# MAGIC 2. List files from demo container 
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl0123.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl0123.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


