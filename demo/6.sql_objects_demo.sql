-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Objectives - databases
-- MAGIC 1. spark sql documentation
-- MAGIC 2. Create Database Demo
-- MAGIC 3. Data tab in UI
-- MAGIC 4. SHOW command
-- MAGIC 5. Describe command
-- MAGIC 6. Find the current database

-- COMMAND ----------

create DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE database demo;

-- COMMAND ----------

describe database EXTENDED demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables;

-- COMMAND ----------

use demo;
show tables in demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Objectives- Managed tables
-- MAGIC 1. create managed tables using python 
-- MAGIC 2. create managed tables using Sql
-- MAGIC 3. Effects of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').saveAsTable('demo.race_result_python')

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

describe EXTENDED demo.race_result_python;

-- COMMAND ----------

select * from 
  demo.race_result_python 
 where race_year = 2020 

-- COMMAND ----------

create table IF NOT EXISTS demo.race_result_sql
AS
select * 
  from demo.race_result_python 
 where race_year = 2020 

-- COMMAND ----------

describe extended demo.race_result_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

Drop table demo.race_results_python;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Objectives- External tables
-- MAGIC 1. create external tables using python 
-- MAGIC 2. create external tables using Sql
-- MAGIC 3. Effects of dropping a external table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').mode('overwrite').option("path", f"{presentation_folder_path}/race_results_ext_py") \
-- MAGIC .saveAsTable("demo.race_result_ext_py")

-- COMMAND ----------

desc EXTENDED demo.race_result_ext_py;

-- COMMAND ----------

create table demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points float,
position INT,
created_date TIMESTAMP
)

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * from demo.race_result_ext_py where race_year = 2020;

-- COMMAND ----------

describe extended demo.race_results_ext_sql;

-- COMMAND ----------

show tables;

-- COMMAND ----------

drop TABLE demo.race_result_ext_py;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on Tables
-- MAGIC 1. create temp view
-- MAGIC 2. create globaltemp view
-- MAGIC 3. create permanent view

-- COMMAND ----------

CREATE OR REPLACE Temp VIEW v_race_results
as 
select * 
  from demo.race_result_python 
where race_year = 2018

-- COMMAND ----------

select * from v_race_results 

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * 
  FROM demo.race_result_python 
where race_year = 2012 

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT * 
  FROM demo.race_result_python 
where race_year = 2000

-- COMMAND ----------

SELECT * FROM demo.pv_race_results

-- COMMAND ----------

SHOW tables;
