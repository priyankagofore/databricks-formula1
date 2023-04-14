# Databricks notebook source
dbutils.widgets.text('p_file_date',"2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read all data into DataFrames

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

drivers_df= spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("name","driver_name") \
.withColumnRenamed("number","driver_number") \
.withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name","team")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location","circuits_location")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")\
.filter(f"file_date = '{v_file_date}'")\
.withColumnRenamed("time","race_time")\
.withColumnRenamed("race_id","results_race_id")\
.withColumnRenamed("file_date","result_file_date")

# COMMAND ----------

# joining Race and circuits dataframes
race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id,"inner") \
.select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_id,circuits_df.circuits_location)

# COMMAND ----------

#joining race_circuits dataframe, drivers and constructors with results dataframe
race_results_df = results_df.join(race_circuits_df, results_df.results_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)      

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id","race_year", "race_name", "race_date", "circuits_location", "driver_name", "driver_number","driver_nationality", "team", "grid", "fastest_lap", "race_time", "points","position","result_file_date") \
.withColumn("created_date",current_timestamp()) \
.withColumnRenamed("result_file_date","file_date")  

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation','race_result','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC --Drop TABLE f1_presentation.race_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_result;
