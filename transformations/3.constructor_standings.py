# Databricks notebook source
dbutils.widgets.text('p_file_date',"2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Produce constructor Standings

# COMMAND ----------

# MAGIC %run "../includes/configuration/"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")\
.filter('race_date = "2021-03-21"')

#.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df,'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import aggregate, count, col, when,sum

# COMMAND ----------

constructor_standing_df = race_results_df.groupBy("race_year","team") \
.agg(sum("points").alias("total_points"), 
 count(when(col("position")== 1, True)).alias ("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank,asc

constructorRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"),(desc("wins")))
final_df = constructor_standing_df.withColumn("rank",rank().over(constructorRankSpec))

# COMMAND ----------

overwrite_partition(final_df,'f1_presentation','constructor_standings','race_year')

# COMMAND ----------


