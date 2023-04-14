# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Datalake containers for the project

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
    #Get the secrets for key-vault
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-clientid')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-tenantid')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-client-secret')

    # set spark configs
    configs = {"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
        # To check mounted or not
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
        
    # Mount the storage account container     
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC Mounts all containers -raw, processed, presentation

# COMMAND ----------

mount_adls('formula1dl012','raw')

# COMMAND ----------

mount_adls('formula1dl012','processed')

# COMMAND ----------

mount_adls('formula1dl012','presentation')

# COMMAND ----------

mount_adls('formula1dl012','demo')

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl012/demo/")

# COMMAND ----------


