-- Databricks notebook source
create DATABASE  if not exists f1_processed
LOCATION "/mnt/formula1dl012/processed"

-- COMMAND ----------

desc DATABASE f1_raw;

-- COMMAND ----------

desc DATABASE f1_processed;

-- COMMAND ----------


