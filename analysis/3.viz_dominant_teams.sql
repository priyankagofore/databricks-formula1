-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE or REPLACE temp view v_dominant_teams
as
SELECT team_name, 
        count(1) as total_races,
        sum(calculated_points) as total_points,
        avg(calculated_points) as avg_points,
        rank() OVER(order By avg(calculated_points)desc) team_rank
from f1_presentations.calculated_race_results
---where race_year BETWEEN 2001 and 2021 
group by team_name 
having count(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT * from v_dominant_teams;

-- COMMAND ----------

SELECT 
        race_year,
        team_name,
        count(1) as total_races,
        sum(calculated_points) as total_points,
        avg(calculated_points) as avg_points
from f1_presentations.calculated_race_results
where team_name IN(SELECT team_name FROM v_dominant_teams where team_rank <= 10)
group by race_year, team_name 
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

desc  v_dominant_teams

-- COMMAND ----------

SELECT race_year, 
        team_name, 
        count(1) as total_races,
        sum(calculated_points) as total_points,
        avg(calculated_points) as avg_points 
  from f1_presentations.calculated_race_results 
 where team_name in (select team_name from v_dominant_teams where team_rank <= 10) 
group by race_year,team_name 
ORDER BY race_year, avg_points DESC
