# Databricks notebook source
# MAGIC %md
# MAGIC #### Read all data into DataFrames

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

drivers_df= spark.read.parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructor")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

results_df = spark.read.parquet("{processed_folder_path}/results")
