# Databricks notebook source
# MAGIC %md
# MAGIC #### step1- Read the json file using spark Dataframe Reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, DoubleType, IntegerType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId",IntegerType(),False),
                                      StructField("raceId",IntegerType(),True),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("constructorId",IntegerType(),True),
                                      StructField("number",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                      StructField("q1",StringType(),True),
                                      StructField("q2",StringType(),True),
                                      StructField("q3",StringType(),True),

])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option('multiline',True).json(f'{raw_folder_path}/qualifying')

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumn("v_data_source", lit(v_data_source)) 

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Write the output to the processed container using Dataframe Writer API

# COMMAND ----------

qualifying_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/qualify")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/qualify'))

# COMMAND ----------

dbutils.notebook.exit("Success")