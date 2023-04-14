-- Databricks notebook source
select * from f1_presentations.calculated_race_results;

-- COMMAND ----------

SELECT team_name as dominant_teams,
        count(1) as total_races,
        sum(calculated_points) as total_points,
        avg(calculated_points) as avg_points
from f1_presentations.calculated_race_results
where race_year BETWEEN 2001 and 2021 
group by team_name 
having count(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------


