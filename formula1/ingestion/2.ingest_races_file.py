# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest races.csv

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

races_schema = StructType([StructField('raceId', IntegerType(), False),
                             StructField('year', IntegerType(), True),
                             StructField('round', IntegerType(), True),
                             StructField('circuitId', IntegerType(), True),
                             StructField('name', StringType(), True),
                             StructField('date', StringType(), True),
                             StructField('time', StringType(), True),
                             StructField('url', StringType(), True)])

# COMMAND ----------

races_df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/races.csv', header=True, schema=races_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##select required columns and rename them

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, to_timestamp, concat, lit

# COMMAND ----------

races_selected_df = races_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'),
                                   col('round'), col('circuitId').alias('circuit_id'), col('name'), col('date'),
                                    col('time'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##add new columns(race_timestamp, ingestion_timestamp)

# COMMAND ----------

races_added_col_df = races_selected_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
.withColumn('data_source', lit(v_data_source))\
.withColumn('file_date', lit(v_file_date))\
.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##drop the date and time columns

# COMMAND ----------

races_final_df = races_added_col_df.drop('date','time')

# COMMAND ----------

# MAGIC %md
# MAGIC ##write the final df to datalake

# COMMAND ----------

#races_final_df.write.option("path", f'{processed_folder_path}/races').mode('overwrite').partitionBy('race_year').format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


