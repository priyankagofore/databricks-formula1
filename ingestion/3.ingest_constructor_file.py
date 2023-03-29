# Databricks notebook source
# MAGIC %md 
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-1 Read the JSON file using spark dataframe reader

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json("/mnt/formula1dl012/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Drop the unwanted columns from the DataFrame

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Rename columns and add ingestion date 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write output to Parquet file

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet("/mnt/formula1dl01/processed/constructor")
