-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed 
LOCATION "/mnt/formula1dl012/processed"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentations CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dl012/presentation"
