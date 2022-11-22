# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest circuits.csv

# COMMAND ----------

# MAGIC %run ../includes/config

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ##read csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType([StructField('circuitId', IntegerType(), False),
                             StructField('circuitRef', StringType(), True),
                             StructField('name', StringType(), True),
                             StructField('location', StringType(), True),
                             StructField('country', StringType(), True),
                             StructField('lat', DoubleType(), True),
                             StructField('lng', DoubleType(), True),
                             StructField('alt', IntegerType(), True),
                             StructField('url', StringType(), True)])

# COMMAND ----------

circuits_df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/circuits.csv', header=True, schema = circuits_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##select only required columns

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df= circuits_df.select(col('circuitId'),col('circuitRef'),col('name'),
                                         col('location'),col('country'),col('lat'),col('lng'),col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##rename columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId','circuit_id')\
.withColumnRenamed('circuitRef','circuit_ref')\
.withColumnRenamed('lat','latitude')\
.withColumnRenamed('lng','longitude')\
.withColumnRenamed('alt','altitude')\
.withColumn('data_source', lit(v_data_source))\
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##add new column

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##write dataframe to datalake

# COMMAND ----------

circuits_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')

# COMMAND ----------

dbutils.notebook.exit("Success")
