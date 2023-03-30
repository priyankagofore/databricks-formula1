# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
.filter("circuit_id < 70")\
.withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("name","race_name")\
.filter('race_year =2019') 

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# inner join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(races_df.race_name,races_df.round,circuits_df.circuit_id,circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.select("circuit_name").show()

# COMMAND ----------

# left outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(races_df.race_name,races_df.round,circuits_df.circuit_id,circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# right outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(races_df.race_name,races_df.round,circuits_df.circuit_id,circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# full outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(races_df.race_name,races_df.round,circuits_df.circuit_id,circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Semi join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
.select(circuits_df.circuit_id,circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#Anti join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#cross-join
race_circuits_df = circuits_df.crossJoin(races_df) 

# COMMAND ----------

display(race_circuits_df)
