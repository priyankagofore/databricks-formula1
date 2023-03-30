# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Filtering using Sql like syntax where and =

# COMMAND ----------

races_filtered_df = races_df.where("race_year = 2019 and round <= 5" )

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Filtering using python like syntax filter and ==

# COMMAND ----------

races_filtered_df = races_df.filter((races_df["race_year"]== 2020) & (races_df["round"] <=5))

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------


