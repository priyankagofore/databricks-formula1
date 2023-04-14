-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

create table f1_presentations.calculated_race_results
using parquet
as
select 
      races.race_year,
      constructors.name as team_name, 
      drivers.name as driver_name, 
      results.position,
      results.points,
      11- results.position AS calculated_points
  from results  
  join f1_processed.drivers on (results.driver_id = drivers.driver_id)
  join f1_processed.constructors on (results.constructor_id = constructors.constructor_id)
  join f1_processed.races on (results.race_id = races.race_id)
where results.position <= 10

-- COMMAND ----------

select * from f1_presentations.calculated_race_results;

-- COMMAND ----------


