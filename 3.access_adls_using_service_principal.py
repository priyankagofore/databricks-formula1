# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Datalake using service principal
# MAGIC Steps to follow
# MAGIC 1. Register Azure AD application/service principal 
# MAGIC 2. Generate a secret/password for an application
# MAGIC 3. set spark config with App/client id, directory/Tenant id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data contributor' to Data lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-client-secret')

# COMMAND ----------

#client_id ="7d6b67ec-2725-4547-8beb-d272d9e98e3f"
#tenant_id ="4b4e036d-f94b-4209-8f07-6860b3641366"
#client_secret ="hqK8Q~mDsUC3pmHV6dfcRI8HkeEXCOlkuN0hodBj"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl0123.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl0123.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl0123.dfs.core.windows.net",client_id )
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl0123.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl0123.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl0123.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl0123.dfs.core.windows.net/circuits.csv"))
