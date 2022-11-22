# Databricks notebook source
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
# MAGIC #Ingest qualifying folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##read json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualify_schema = StructType([StructField('qualifyId', IntegerType(), False),
                               StructField('raceId', IntegerType(), True),
                               StructField('driverId', IntegerType(), True),
                               StructField('constructorId', IntegerType(), True),
                               StructField('number', IntegerType(), True),
                               StructField('position', IntegerType(), True),
                               StructField('q1', StringType(), True),
                               StructField('q2', StringType(), True),
                               StructField('q3', StringType(), True)])

# COMMAND ----------

qualify_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/qualifying', schema = qualify_schema, multiLine = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualify_final_df = qualify_df.withColumn('data_source', lit(v_data_source)) \
                                    .withColumn('data_source', lit(v_file_date)) \
                                    .withColumn('ingestion_date', current_timestamp()) \
                                    .withColumnRenamed('driverId', 'driver_id') \
                                    .withColumnRenamed('raceId', 'race_id') \
                                    .withColumnRenamed('qualifyId', 'qualify_id') \
                                    .withColumnRenamed('constructorId', 'constructor_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ##write output to parquet file

# COMMAND ----------

#overwrite_partition(qualify_final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualify_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
