# Databricks notebook source
# MAGIC %md
# MAGIC ####### Ingest multiple laptimes csv file

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

from pyspark.sql.types import StringType, StructField, StructType, DoubleType, IntegerType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("lap",IntegerType(),True),
                                      StructField("position",StringType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f'{raw_folder_path}/lap_times/lap_times_split*.csv')

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("driverId","driver_id") \
                     .withColumnRenamed("raceId","race_id") \
                     .withColumn("v_data_source", lit(v_data_source))   

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Write the output to the processed container using Dataframe Writer API

# COMMAND ----------

lap_times_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.lap_times")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/lap_times'))

# COMMAND ----------

dbutils.notebook.exit("Success")
