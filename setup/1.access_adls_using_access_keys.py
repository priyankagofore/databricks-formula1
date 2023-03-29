# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Datalake using Access Keys 
# MAGIC 1. set the spark config to fs.azure.account.key 
# MAGIC 2. List files from demo container 
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key= dbutils.secrets.get(scope= 'formula1-scope',key= 'formula1-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dl012.dfs.core.windows.net",
    formula1dl_account_key)
   

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl012.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl012.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


