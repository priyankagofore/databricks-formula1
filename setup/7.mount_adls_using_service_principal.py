# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Datalake using service principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id,tenant_id and client_server from key vault
# MAGIC 2. set spark config with App/client id, directory/Tenant id & Secret
# MAGIC 3. call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount(lists all mount and unmounts)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-clientid')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-tenantid')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dl012.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl012/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl012/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl012/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dl012/demo")

# COMMAND ----------


