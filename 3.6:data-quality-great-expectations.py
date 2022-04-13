# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Tests using Databricks and Great Expectations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Examples of Data Quality checks, or "expectations" you can perform
# MAGIC ---
# MAGIC 
# MAGIC <img src="https://mchanstorage2.blob.core.windows.net/mchan-images/Screenshot 2022-04-13 at 11.38.29 PM.png" width="800"> 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Examples of Data Quality checks, or "expectations" you can perform
# MAGIC ---
# MAGIC 
# MAGIC <img src="https://mchanstorage2.blob.core.windows.net/mchan-images/expectations_01.png" width="900"> 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Setup Great Expectations

# COMMAND ----------

import datetime as datetime 
from pyspark.sql.functions import col

# COMMAND ----------

# Install Great Expectations as a notebook-scoped library 
%pip install great-expectations

# COMMAND ----------

from ruamel import yaml

from great_expectations.core.batch import BatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

# COMMAND ----------

root_directory = "/dbfs/mnt/mchan-great-expectations/ge-root"
data_context_config = DataContextConfig(
    store_backend_defaults = FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config = data_context_config)

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

my_spark_datasource_config = """
name: nyc_taxi_trips
class_name: Datasource
execution_engine:
  class_name: SparkDFExecutionEngine
data_connectors:
  insert_your_data_connector_name_here:
    module_name: great_expectations.datasource.data_connector
    class_name: RuntimeDataConnector
    batch_identifiers:
      - some_key_maybe_pipeline_stage
      - some_other_key_maybe_run_id
"""

# COMMAND ----------

context.test_yaml_config(my_spark_datasource_config)

# COMMAND ----------

context.add_datasource(**yaml.load(my_spark_datasource_config))

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name = "nyc_taxi_trips",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="df_great_expectations",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
        "some_key_maybe_pipeline_stage": "prod",
        "some_other_key_maybe_run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3. Create Expectations

# COMMAND ----------

# Define the expectation suite name
expectation_suite_name = "mchan_great_expectation_suite"

context.create_expectation_suite(
    expectation_suite_name = expectation_suite_name, overwrite_existing=True
)
validator = context.get_validator(
    batch_request = batch_request,
    expectation_suite_name=expectation_suite_name,
)

print(validator.head())

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4. Make Data Quality Checks 
# MAGIC ---
# MAGIC - Expectation 1: ```passenger_count``` should not be NULL 
# MAGIC - Expectation 2: range of values for Taxi fares to be between ```$1``` and ```$1000```

# COMMAND ----------

# Test 1: Check the [passenger_count] column and check for the existence of NULL values 
validator.expect_column_values_to_not_be_null(column="passenger_count")

# COMMAND ----------

# Test 2: Check the [total_amount] column if it's in between $0 and $1000
# There were 23 records or taxi trips that violated this condition 
validator.expect_column_values_to_be_between(
    column="total_amount", min_value = 0, max_value = 1000
)

# COMMAND ----------

my_checkpoint_name = "mchan_great_expectations_checkpoint" 

my_checkpoint_config = f"""
    name: {my_checkpoint_name}
    config_version: 1.0
    class_name: SimpleCheckpoint
    run_name_template: "%Y%m%d-%H%M%S-data-quality-run"
"""

# COMMAND ----------

my_checkpoint = context.test_yaml_config(my_checkpoint_config)

# COMMAND ----------

context.add_checkpoint(**yaml.load(my_checkpoint_config))

# COMMAND ----------

checkpoint_result = context.run_checkpoint(
    checkpoint_name = "mchan_great_expectations_checkpoint",
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
)
