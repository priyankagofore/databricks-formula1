-- Databricks notebook source
create database IF NOT EXISTS f1_presentations
LOCATION "/mnt/formula1dl012/presentation"

-- COMMAND ----------

desc database EXTENDED f1_presentations;

-- COMMAND ----------

drop table f1_presentation.driver_standings;

-- COMMAND ----------

drop DATABASE f1_presentation;

-- COMMAND ----------


