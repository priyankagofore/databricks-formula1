# Databricks notebook source
# MAGIC %md 
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-1 Read the JSON file using spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json(f"{raw_folder_path}/constructors.json")

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

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").withColumn("v_data_source", lit(v_data_source))

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write output to Parquet file

# COMMAND ----------

constructor_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
