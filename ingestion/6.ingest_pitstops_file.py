# Databricks notebook source
# MAGIC %md
# MAGIC ####### Ingest PitStops.JSON file

# COMMAND ----------

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

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("stop",StringType(),True),
                                      StructField("lap",IntegerType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("duration",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)
])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option('multiline',True).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("raceId","race_id") \
.withColumn("v_data_source",lit(v_data_source))    
  

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Write the output to the processed container using Dataframe Writer API

# COMMAND ----------

pit_stops_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")
