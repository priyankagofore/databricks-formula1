# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{processed_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import aggregate,sum,col,count,when

# COMMAND ----------

drivers_standing_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"), 
 count(when(col("position")== 1, True)).alias ("wins"))

# COMMAND ----------

display(drivers_standing_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = drivers_standing_df.withColumn("rank", rank().over(driverRankSpec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_presentations.driver_standings")

# COMMAND ----------


