# Databricks notebook source
# MAGIC %pip install delta-sharing

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Download Sample Profile

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/mendelsohn.chan/")

# COMMAND ----------

import sys 
"wget https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/delta-sharing/share/open-datasets.share"
dbutils.fs.cp("file:/tmp/open-datasets.share", "dbfs:/mendelsohn.chan/")

# COMMAND ----------

dbutils.fs.cp("file:/tmp/open-datasets.share", "dbfs:/FileStore/")

# COMMAND ----------

dbutils.fs.cp("/FileStore/open-datasets.share", "/Users/mendelsohn.chan/Desktop/DeltaSharingShare")

# COMMAND ----------

# list the files in /mendelsohn.chan
dbutils.fs.ls("/mendelsohn.chan/")

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/delta-sharing/share/open-datasets.share -O /tmp/open-datasets.share

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Share Client 

# COMMAND ----------

import delta_sharing

# Point to the profile file.
# It can be a file on the local file system or a file on a remote storage.
profile_file = "/tmp/open-datasets.share"

# Create a SharingClient
client = delta_sharing.SharingClient(profile_file)

# List all shared tables
available_tables = client.list_all_tables()
print("Listing available tables:")
for table in available_tables:
  print(table.share + "." + table.schema + "." + table.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load the COVID-19 Data Set as a Pandas DataFrame

# COMMAND ----------

# Create a url to access a shared table.
# A table path is the profile file path following with `#` and 
# the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + "#delta_sharing.default.COVID_19_NYT"

# Load a table as a Pandas DataFrame. This can be used to process tables that can fit in the memory.
pandasDF = delta_sharing.load_as_pandas(table_url)

# COMMAND ----------

pandasDF.head()
