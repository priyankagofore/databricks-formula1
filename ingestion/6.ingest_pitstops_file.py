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

dbutils.widgets.text('p_file_date',"2021-03-28")
v_file_date = dbutils.widgets.get('p_file_date')

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

pit_stops_df = spark.read.schema(pit_stops_schema).option('multiline',True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("raceId","race_id") \
.withColumn("v_data_source",lit(v_data_source))   \
.withColumn("file_date",lit(v_file_date))     
  

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Write the output to the processed container using Dataframe Writer API

# COMMAND ----------

#overwrite_partition(pit_stops_final_df,'f1_processed','pit_stops','race_id')

# COMMAND ----------

merge_condition= " tgt.race_id = src.race_id AND tgt.driver_id= src.driver_id AND tgt.stop= src.stop AND tgt.race_id = src.race_id "
merge_delta_data(pit_stops_final_df, 'f1_processed', 'pitstops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC from f1_processed.pitstops
# MAGIC group by race_id 
# MAGIC ORDER BY race_id desc;

# COMMAND ----------


