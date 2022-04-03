# Databricks notebook source
# MAGIC %md
# MAGIC # Mount DBFS on top of ADLS using KeyVault

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Go to Azure Key Vault and then place your ADLS storage key in a secret

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://mchanstorage2.blob.core.windows.net/mchan-images/kv_01.png" width="500"> 

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://mchanstorage2.blob.core.windows.net/mchan-images/kv_02.png" width="1000"> 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create a Secret Scope in the Databricks Workspace

# COMMAND ----------

# PATTERN 
https://<DATABRICKS-INSTANCE>#secrets/createScope

# Example 
https://adb-6464564345757588.8.azuredatabricks.net/?o=6464564345757588#secrets/createScope

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://mchanstorage2.blob.core.windows.net/mchan-images/Screenshot 2022-01-19 at 11.31.45 PM.png" width="500"> 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 3: Mount DBFS on top of ADLS 

# COMMAND ----------

dbutils.fs.mount(
      source = "wasbs://mchan-test@mchanstorage2.blob.core.windows.net",
      mount_point = "/mnt/mchan-test", 
      extra_configs = {"fs.azure.account.key.mchanstorage2.blob.core.windows.net":dbutils.secrets.get(scope = "mchanstorage2-secretscope", key = "mchanstorage2-key")}
)
