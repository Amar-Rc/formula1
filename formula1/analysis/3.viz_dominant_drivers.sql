-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style=color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view dominant_drivers_v
as
select driver_name,  
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) as driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having total_races >= 50
order by avg_points desc

-- COMMAND ----------

select race_year,
driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points, 
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from dominant_drivers_v where driver_rank <=10)
and race_year between 2000 and 2020
group by race_year, driver_name
order by race_year, avg_points desc

-- COMMAND ----------

select race_year,
driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points, 
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from dominant_drivers_v where driver_rank <=10)
group by race_year, driver_name
order by race_year, avg_points desc
