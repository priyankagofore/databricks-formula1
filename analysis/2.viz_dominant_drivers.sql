-- Databricks notebook source
CREATE or REPLACE temp view v_dominant_drivers
as
SELECT 
        driver_name,
        count(1) as total_races,
        sum(calculated_points) as total_points,
        avg(calculated_points) as avg_points,
        rank() OVER(order By avg(calculated_points)desc) driver_rank
from f1_presentations.calculated_race_results
-- where race_year BETWEEN 2001 and 2021 
group by driver_name 
having count(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT 
        race_year,
        driver_name,
        count(1) as total_races,
        sum(calculated_points) as total_points,
        avg(calculated_points) as avg_points
  from f1_presentations.calculated_race_results
 where driver_name IN (select driver_name from v_dominant_drivers where driver_rank <= 10)
group by race_year, driver_name 
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

desc  v_dominant_drivers

-- COMMAND ----------

SELECT race_year, 
        driver_name, 
        count(1) as total_races,
        sum(calculated_points) as total_points,
        avg(calculated_points) as avg_points 
  from f1_presentations.calculated_race_results 
 where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10) 
group by race_year, driver_name 
ORDER BY avg_points DESC, race_year  desc

-- COMMAND ----------

SELECT race_year, 
        driver_name, 
        count(1) as total_races,
        sum(calculated_points) as total_points,
        avg(calculated_points) as avg_points 
  from f1_presentations.calculated_race_results 
 where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10) 
group by race_year, driver_name 
ORDER BY avg_points DESC, race_year  desc

-- COMMAND ----------


