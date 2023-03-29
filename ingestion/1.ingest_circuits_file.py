# Databricks notebook source
# MAGIC %md
# MAGIC ###### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### step1 - Read circuits.csv using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read \
.option("header",True)\
.schema(circuits_schema)\
.csv("/mnt/formula1dl012/raw/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-2 selected only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df= circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(cicuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-3 Renaming the selected columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-4 Add ingestion date to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step-5 write data to datelake as Parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1dl012/processed/circuits")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl012/processed/circuits"))

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl012/processed/circuits"))
