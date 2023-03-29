# Databricks notebook source
# MAGIC %md 
# MAGIC Race circuit csv file into parquet

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-1 Read race.csv file using spark  DataFrame Reader

# COMMAND ----------

from  pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField,TimestampType,DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField(("raceId"),IntegerType(),False ),
StructField(("year"),IntegerType(), True),
StructField(("round"),IntegerType(), True),
StructField(("circuitId"),IntegerType(), True),
StructField(("name"),StringType(), True),
StructField(("date"),DateType(), True),
StructField(("time"),StringType(), True),
StructField(("url"),StringType(), True)
])

# COMMAND ----------

races_df= spark.read.option("header",True).schema(races_schema).csv("/mnt/formula1dl012/raw/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-2 Add ingestion date and timestamp to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,current_date,to_timestamp,concat,lit,col

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-3 Selecting and Renaming only the Required columns

# COMMAND ----------

races_selected_df= races_with_timestamp_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("race_timestamp"),col("ingestion_date"))

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId","race_id").withColumnRenamed("year", "race_year").withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-4 Writing  output to Processed container in Parquet format

# COMMAND ----------

races_renamed_df.write.mode('overwrite').parquet('/mnt/formula1dl012/processed/races')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dl012/processed/races'))

# COMMAND ----------

races_renamed_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1dl012/processed/races')
