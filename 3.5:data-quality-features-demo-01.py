# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Data Quality Features 
# MAGIC ----
# MAGIC #### 1. Schema Enforcement  
# MAGIC #### 2. Schema Evolution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 1: Schema Enforcement 
# MAGIC ---
# MAGIC - A safeguard in that ensures data quality by rejecting ```writes``` that do not match the target end-table’s schema
# MAGIC - Example 1: the presence of a new, rogue column
# MAGIC - Example 2: mismatch in data types (e.g. final date column is a ```DATE``` but the new data being appended is a ```STRING```)

# COMMAND ----------

silverTblPath = "dbfs:/user/hive/warehouse/mchan_data_quality.db/silver_payment_transactions"

df.write\
	  .mode("overwrite")\
	  .format("delta")\
	  .save(silverTblPath)

# COMMAND ----------

# DBTITLE 1,1. Read the current state of the silver, cleansed table 
df = spark.read.table("mchan_data_quality.silver_payment_transactions")
display(df)

# COMMAND ----------

# DBTITLE 1,2. Print the schema of the silver, cleansed table 
df.printSchema()

# COMMAND ----------

# DBTITLE 1,3. Now, let's ingest new data that doesn't match the final table's schema (date not casted properly, missing 1 field)
new_data = spark.read\
               .format("csv")\
               .option("header", "true")\
               .option("inferSchema", "true")\
               .load("dbfs:/FileStore/02_raw_payment_transactions.csv")\
               .select("date_transaction","row_id", "loan_id", "currency", "funded_amount", "paid_amount")
display(new_data) 

# COMMAND ----------

# DBTITLE 1,4. Now let's try to write the new data by appending it to the existing final table
new_data.write.format("delta").mode("append").saveAsTable("mchan_data_quality.silver_payment_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 2: Schema Evolution
# MAGIC ---
# MAGIC - Allows users to change a table's current schema to accommodate data that is changing over time
# MAGIC - Simply add the following to the Spark ```write``` command
# MAGIC - ```.option("mergeSchema", "true")```

# COMMAND ----------

# DBTITLE 1,1. Example final table that we want to update by appending new data to it
t2 = spark.read.table("mchan_data_quality.schema_evolution_final")
display(t2) 

# COMMAND ----------

# DBTITLE 1,2. Let us try to append this new table with a new column called [rogue_column] 
newDataWithNewColumn = spark.read\
               .format("csv")\
               .option("header", "true")\
               .option("inferSchema", "true")\
               .load("dbfs:/FileStore/02_raw_payment_transactions.csv")\
               .select("row_id", "loan_id", "paid_amount", "rogue_column")
display(newDataWithNewColumn)

# COMMAND ----------

# DBTITLE 1,3. Write to the new data by using the merge schema option and query the final resulting table 
newDataWithNewColumn.write\
        .format("delta")\
        .mode("append")\
        .option("mergeSchema", "true")\
        .saveAsTable("mchan_data_quality.schema_evolution_final")

# COMMAND ----------

display(spark.read.table("mchan_data_quality.schema_evolution_final").orderBy("row_id"))

# COMMAND ----------

# MAGIC %md 
# MAGIC For more information, refer to the docs [here](https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html)
