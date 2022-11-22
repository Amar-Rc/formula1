# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest results.json file

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run ../includes/config

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

results_schema = 'resultId INT NOT NULL, raceId INT, driverId INT, constructorId INT, number INT, \
grid INT, position INT, positionText STRING, positionOrder INT, \
points FLOAT, laps INT, time STRING, milliseconds INT, fastestLap INT, \
rank INT, fastestLapTime STRING, fastestLapSpeed STRING, statusId INT'

# COMMAND ----------

results_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/results.json', schema = results_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_col_df = results_df.withColumn('data_source', lit(v_data_source)) \
                                .withColumn('file_date', lit(v_file_date)) \
                                .withColumn('ingestion_date', current_timestamp()) \
                                .withColumnRenamed('resultId', 'result_id') \
                                .withColumnRenamed('raceId', 'race_id') \
                                .withColumnRenamed('driverId', 'driver_id') \
                                .withColumnRenamed('constructorId', 'constructor_id') \
                                .withColumnRenamed('positionText', 'position_text') \
                                .withColumnRenamed('positionOrder', 'position_order') \
                                .withColumnRenamed('fastestLap', 'fastest_lap') \
                                .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
                                .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')

# COMMAND ----------

# MAGIC %md
# MAGIC ##drop unwanted column

# COMMAND ----------

results_final_df = results_with_col_df.drop('statusId')

# COMMAND ----------

results_dedup_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##write output to parquet file

# COMMAND ----------

#overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_dedup_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


