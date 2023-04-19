# Databricks notebook source
dbutils.widgets.text('p_file_date',"2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

spark.sql(f"""
            create table IF NOT EXISTS f1_presentation.calculated_race_results
            (
            race_year INT,
            team_name STRING,
            driver_id INT,
            driver_name STRING,
            race_id INT,
            position INT,
            points INT,
            calculated_points INT,
            created_date TIMESTAMP,
            updated_date TIMESTAMP
            )
            using DELTA
""")

# COMMAND ----------

spark.sql(f""" 
                CREATE OR REPLACE TEMP VIEW race_results_updated
                AS 
                SELECT  races.race_year,
                        constructors.name AS team_name,
                        drivers.driver_id,
                        drivers.name AS driver_name,
                        races.race_id,
                        results.position,
                        results.points,
                        11-results.position AS calculated_points
                    from f1_processed.results
                    join f1_processed.drivers on (results.driver_id = drivers.driver_id)
                    join f1_processed.constructors on (results.constructor_id = constructors.constructor_id)
                    join f1_processed.races on (results.race_id = races.race_id)
                 where results.position <= 10
                    AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

spark.sql(f"""
MERGE into f1_presentation.calculated_race_results tgt 
using race_results_updated src
on (tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id)
when matched then
  update set tgt.position = src.position,
             tgt.points = src.points,
             tgt.calculated_points = src.calculated_points,
             tgt.updated_date = current_timestamp
when not matched 
  then insert (race_year,team_name,driver_id,driver_name,race_id,position,points,calculated_points,created_date)
        values(race_year,team_name,driver_id,driver_name,race_id,position,points,calculated_points,current_timestamp)  
""")          


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from race_results_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from f1_presentation.calculated_race_results;
