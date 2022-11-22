# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Constructors.json file

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

constructor_schema = "constructorId INT NOT NULL, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/constructors.json', schema=constructor_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##drop unwanted columns

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed('constructorId', 'constructor_id') \
                                             .withColumnRenamed('constructorRef', 'constructor_ref') \
                                             .withColumn('data_source', lit(v_data_source)) \
                                             .withColumn('file_date', lit(v_file_date))\
                                             .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit("Success")
