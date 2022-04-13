# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Tests using Databricks and Great Expectations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Setup Great Expectations

# COMMAND ----------

# Install Great Expectations as a notebook-scoped library 
%pip install great-expectations

# COMMAND ----------

# After that we will take care of some imports that will be used later. Choose your configuration options to show applicable imports:
from ruamel import yaml
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

# COMMAND ----------

# path to root where we store data for Metadata Stores 
rootDirectory = "/mnt/mchan-great-expectations/ge-root-directory/"

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory = rootDirectory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2. Connect and Read the Data Set 

# COMMAND ----------

# Read the CSV file for this demo as a Spark DataFrame 
df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/mnt/mchan-great-expectations/raw-data/")
display(df) 

# COMMAND ----------

datasource_yaml = r"""
name: taxi_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetDBFSDataConnector	
    base_directory: /mnt/mchan-great-expectations/raw-data/
    glob_directive: "*.csv"
    default_regex:
      group_names:
        - data_asset_name
        - year
        - month
      pattern: (.*)/.*(\d{4})-(\d{2})\.csv
"""

# COMMAND ----------

# Add the data source
context.test_yaml_config(datasource_yaml)

# COMMAND ----------


