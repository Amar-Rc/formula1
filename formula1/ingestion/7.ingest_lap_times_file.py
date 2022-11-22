# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest lap_times folder

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

lap_times_schema = "raceId INT NOT NULL, driverId INT, lap INT, position INT, time STRING, milliseconds INT"

# COMMAND ----------

lap_times_df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/lap_times/', schema = lap_times_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##rename columns and add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumn('data_source', lit(v_data_source)) \
                                 .withColumn('data_source', lit(v_file_date)) \
                                 .withColumn('ingestion_date', current_timestamp()) \
                                 .withColumnRenamed('driverId', 'driver_id') \
                                 .withColumnRenamed('raceId', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ##write output to parquet file

# COMMAND ----------

#overwrite_partition(lap_times_final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
