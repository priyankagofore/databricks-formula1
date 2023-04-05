# Databricks notebook source
# MAGIC %md
# MAGIC ####### Ingest Results.JSON file

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,FloatType,DateType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(),False),
                                    StructField("raceId", IntegerType(),True),
                                    StructField("driverId", IntegerType(),True),
                                    StructField("constructorId", IntegerType(),True),
                                    StructField("number", IntegerType(),True),
                                    StructField("grid", IntegerType(),True),
                                    StructField("position", IntegerType(),True),
                                    StructField("positionText", StringType(),True),
                                    StructField("positionOrder", IntegerType(),True),
                                    StructField("points", FloatType(),True),
                                    StructField("laps", IntegerType(),True),
                                    StructField("time", StringType(),True),
                                    StructField("milliseconds", IntegerType(),True),
                                    StructField("fastestLap", IntegerType(),True),
                                    StructField("rank", IntegerType(),True),
                                    StructField("fastestLapTime", StringType(),True),
                                    StructField("fastestLapSpeed", StringType(),True),
                                    StructField("statusId", IntegerType(),True)
])

# COMMAND ----------

result_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

result_renamed_df =result_df.withColumnRenamed("resultId","result_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("positionText",'position_order').withColumnRenamed("positionOrder","positionorder").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapTime","fastest_lap_time").withColumnRenamed("fastestLapSpeed","fastest_lap_speed").withColumn("v_data_source",lit(v_data_source))
                                   

# COMMAND ----------

result_renamed_df = add_ingestion_date(result_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Drop unwanted columns

# COMMAND ----------

results_final_df = result_renamed_df.drop("StatusId")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-4 Write the output to the processed container using Dataframe Writer API

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")
