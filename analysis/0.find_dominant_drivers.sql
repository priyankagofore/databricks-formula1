-- Databricks notebook source
select driver_name,
       count(1) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as  avg_points
from f1_presentations.calculated_race_results
WHERE race_year BETWEEN 2001 and 2020
GROUP BY driver_name
having count(1) >= 50
ORDER BY avg_points DESC


-- COMMAND ----------


