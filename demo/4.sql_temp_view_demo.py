# Databricks notebook source
# MAGIC %md
# MAGIC #### Objectives
# MAGIC 1. Create temp views on dataframe
# MAGIC 2. Access the view from Sql cell
# MAGIC 3. Access the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from v_race_results where race_year = 2020;

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_year_2019_df = spark.sql(f'select * from v_race_results where race_year = {p_race_year} ')

# COMMAND ----------

display(race_year_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global Temp view Objectives
# MAGIC 1. Create temp views on dataframe
# MAGIC 2. Access the view from Sql cell
# MAGIC 3. Access the view from python cell
# MAGIC 4. Access from another notebook

# COMMAND ----------

race_results_df.createGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from global_temp.gv_race_results;

# COMMAND ----------

gv_race_results_df = spark.sql('SELECT * from global_temp.gv_race_results')

# COMMAND ----------

display(gv_race_results_df)

# COMMAND ----------


