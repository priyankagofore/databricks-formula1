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

dbutils.widgets.text('p_file_date',"2021-03-28")
v_file_date = dbutils.widgets.get('p_file_date')

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

result_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-2 Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

result_renamed_df =result_df.withColumnRenamed("resultId","result_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("positionText",'position_text').withColumnRenamed("positionOrder","position_order").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapTime","fastest_lap_time").withColumnRenamed("fastestLapSpeed","fastest_lap_speed").withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))
                                   

# COMMAND ----------

result_renamed_df = add_ingestion_date(result_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Drop unwanted columns

# COMMAND ----------

results_final_df = result_renamed_df.drop("StatusId")

# COMMAND ----------

# MAGIC %md
# MAGIC #### De-dupe the dataframe

# COMMAND ----------

results_deduped_df = results_final_df.drop_duplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-4 Write the output to the processed container using Dataframe Writer API

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1.processed_results")):
#         spark.sql(f"ALTER table f1_processed.results DROP IF EXISTS PARTITION (race_id = (race_id_list.race_id))")

# COMMAND ----------

#results_final_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# %sql
# select race_id,count(1)
# from f1_processed.results
# group by race_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table f1_processed.results;

# COMMAND ----------

# overwrite_partition(results_final_df,'f1_processed','results','race_id')

# COMMAND ----------

merge_condition= "tgt.result_id=src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------



dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,driver_id, count(1)
# MAGIC FROM f1_processed.results 
# MAGIC GROUP BY race_id,driver_id
# MAGIC HAVING count(1) > 1
# MAGIC ORDER BY race_id,driver_id desc;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_processed.results WHERE race_id = 540 and driver_id = 229;

# COMMAND ----------


