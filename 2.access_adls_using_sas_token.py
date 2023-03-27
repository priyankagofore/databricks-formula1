# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Datalake using Access Keys 
# MAGIC 1. set the spark config for SAS token 
# MAGIC 2. List files from demo container 
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1_demo_sas_token= dbutils.secrets.get(scope ='formula1-scope', key = 'formula1-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl0123.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl0123.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl0123.dfs.core.windows.net",formula1_demo_sas_token )
####sp=rl&st=2023-03-23T08:20:19Z&se=2023-03-23T16:20:19Z&spr=https&sv=2021-12-02&sr=c&sig=RNjDMwQcgI1X9UzUG6ugePu4CBGlrq%2F0hb7xb8dJgkk%3D

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl0123.dfs.core.windows.net"))
