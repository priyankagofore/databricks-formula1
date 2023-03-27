# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Datalake using Access Keys 
# MAGIC 1. set the spark config to fs.azure.account.key 
# MAGIC 2. List files from demo container 
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key= dbutils.secrets.get(scope= 'formula1-scope',key= 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dl0123.dfs.core.windows.net",
    formula1dl_account_key)
   ## "mytjG2a/sNFNCRAVuX09WpJZd9SICH4e/IzV7uJj/gAkHXGBXXp2GEbfkYzqQAgx5xuIX9BR5x2Q+AStwi90gw==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl0123.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl0123.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


