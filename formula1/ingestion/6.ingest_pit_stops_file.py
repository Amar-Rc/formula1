# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest pitstops.json file

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

# MAGIC %md
# MAGIC ##read json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType([StructField('raceId', IntegerType(), False),
                             StructField('driverId', IntegerType(), True),
                             StructField('stop', IntegerType(), True),
                             StructField('lap', IntegerType(), True),
                             StructField('time', StringType(), True),
                             StructField('duration', StringType(), True),
                             StructField('milliseconds', IntegerType(), True)])

# COMMAND ----------

pit_stops_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/pit_stops.json', schema = pit_stops_schema, multiLine = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumn('data_source', lit(v_data_source)) \
                                    .withColumn('file_date', lit(v_file_date)) \
                                    .withColumn('ingestion_date', current_timestamp()) \
                                    .withColumnRenamed('driverId', 'driver_id') \
                                    .withColumnRenamed('raceId', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ##write output to parquet file

# COMMAND ----------

#overwrite_partition(pit_stops_final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
