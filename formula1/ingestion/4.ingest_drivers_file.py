# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest drivers.json file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType([StructField("forename", StringType(), True),
                         StructField("surname", StringType(), True)])

# COMMAND ----------

drivers_schema = StructType([StructField('driverId', IntegerType(), True),
                            StructField('driverRef', StringType(), True),
                            StructField('number', IntegerType(), True),
                            StructField('code', StringType(), True),
                            StructField('name', name_schema),
                            StructField('dob', DateType(), True),
                            StructField('nationality', StringType(), True),
                            StructField('url', StringType(), True)])

# COMMAND ----------

drivers_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/drivers.json', schema = drivers_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_column_df = drivers_df.withColumn('data_source', lit(v_data_source)) \
                                   .withColumn('file_date', lit(v_file_date))\
                                   .withColumn('ingestion_date', current_timestamp()) \
                                   .withColumnRenamed('driverId', 'driver_id') \
                                   .withColumnRenamed('driverRef', 'driver_ref') \
                                   .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))

# COMMAND ----------

# MAGIC %md
# MAGIC ##drop unwanted columns

# COMMAND ----------

drivers_final_df = drivers_with_column_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##write output to parquet file

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.drivers')

# COMMAND ----------

dbutils.notebook.exit("Success")
