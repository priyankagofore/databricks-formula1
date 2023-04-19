# Databricks notebook source
# MAGIC %md 
# MAGIC #### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step-1 Read the drivers.json using spart DataFrame reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename",StringType(),True),
                                 StructField("surname",StringType(),True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(),True),
                                    StructField("driverRef", StringType(),True),
                                    StructField("number", IntegerType(),True),
                                    StructField("code", StringType(),True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(),True),
                                    StructField("nationality", StringType(),True),
                                    StructField("url", StringType(),True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-2 Rename columns and add new columns
# MAGIC 1. driverID to driver_id
# MAGIC 2. driverRef to driver_ref
# MAGIC 3. ingestion data added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit,concat

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverID","driver_id")  \
                               .withColumnRenamed("driverRef","driver_ref") \
                               .withColumn("name",concat(col('name.forename'), lit(' '), col('name.surname')) )\
                               .withColumn("data_source", lit(v_data_source))\
                               .withColumn("file_date",lit(v_file_date)) 
                               

# COMMAND ----------

drivers_renamed_df = add_ingestion_date(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step-3 Drop the Unwanted columns
# MAGIC 1. name.forenmae
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step-4 Write the output to the processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.drivers')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")
