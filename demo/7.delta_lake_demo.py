# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake(managed)
# MAGIC 2. Write data to delta lake (external)
# MAGIC 3. Read data from delta lake (table)
# MAGIC 4. Read data from delta lake(file)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1dl012/demo'

# COMMAND ----------

results_df = spark.read\
.option("inferSchema", True)\
.json("/mnt/formula1dl012/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.mode('overwrite').format('delta').saveAsTable("f1_demo.results_managed")

# COMMAND ----------

results_df.write.mode('overwrite').format('delta').save("/mnt/formula1dl012/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1dl012/demo/results_external'

# COMMAND ----------

results_external_df = spark.read.format('delta').load('/mnt/formula1dl012/demo/results_external')

# COMMAND ----------

results_df.write.mode('overwrite').format('delta').partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Updates delta table 
# MAGIC 2. Delete from delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed 
# MAGIC   SET points = 11-position
# MAGIC where position <= 10 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dl012/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update("position >= 10", { "points": "'21-position'" }
)


# COMMAND ----------


