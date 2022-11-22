# Databricks notebook source
#spark.conf.set("fs.azure.account.key.lshcstorageaccount.dfs.core.windows.net", dbutils.secrets.get('lshc-scope', 'lshcstorageaccountkey'))

# COMMAND ----------

storage_account_name = 'lshcstorageaccount'

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": dbutils.secrets.get('lshc-scope', 'lshcstorageaccountkey')})

# COMMAND ----------

mount_adls('raw')
mount_adls('processed')

# COMMAND ----------

mount_adls('demo')

# COMMAND ----------

dbutils.fs.ls('/mnt/lshcstorageaccount/')

# COMMAND ----------

mount_adls('presentation')

# COMMAND ----------



# COMMAND ----------


