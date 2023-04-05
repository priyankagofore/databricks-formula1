-- Databricks notebook source
create database IF NOT EXISTS f1_raw; 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create circuits table

-- COMMAND ----------

DROP table if EXISTS  f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitsId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING  
)
USING CSV
OPTIONS(path "/mnt/formula1dl012/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create Races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING CSV
OPTIONS(path "/mnt/formula1dl012/raw/races.csv", header True)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Constructor tables
-- MAGIC 1. simple line Json
-- MAGIC 2. simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
using JSON
options(path "/mnt/formula1dl012/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Drivers tables
-- MAGIC 1. simple line Json
-- MAGIC 2. complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
using json
options(path "/mnt/formula1dl012/raw/drivers.json")

-- COMMAND ----------

SELECT * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Results tables
-- MAGIC 1. simple line Json
-- MAGIC 2. simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId Int,
number Int,
Grid Int,
position Int,
positonText Int,
positionOrder Int,
points Int,
laps Int,
time String,
milliseconds  Int,
fastestLap Int,
rank Int,
fastestLapTime String,
fastestLapSpeed Float,
statusId String
)
using Json
options(path "/mnt/formula1dl012/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create pitstops tables
-- MAGIC 1. Multi Line Json
-- MAGIC 2. Simple Structure

-- COMMAND ----------

Drop Table IF EXISTS f1_raw.pit_stops;
CREATE table IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time String
)
using JSON
options(path "/mnt/formula1dl012/raw/pit_stops.json", multiline true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for List of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Lap times table
-- MAGIC 1. csv files
-- MAGIC 2. Multiple files

-- COMMAND ----------

Drop table If exists f1_raw.lap_times;
CREATE table IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
using CSV
options(path "/mnt/formula1dl012/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Qualifying table
-- MAGIC 1. Json files
-- MAGIC 2. MultiLine Json
-- MAGIC 3. Multiple files

-- COMMAND ----------

drop table IF EXISTS f1_raw.qualifying;
create table IF NOT EXISTS f1_raw.qualifying(
qualifyId INT,
raceId INT,
driverId INt,
constructorId INT,
number Int,
position Int,
q1 String,
q2 String,
q3 String
)
using JSON
options(path "/mnt/formula1dl012/raw/qualifying", multiline true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;
