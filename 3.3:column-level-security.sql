-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Column-Level Security (aka Dynamic Data Masking)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 0: Understand the data set for the demo

-- COMMAND ----------

DESCRIBE SCHEMA mchan_credit_card;

-- COMMAND ----------

DESCRIBE TABLE dim_customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Examine the table without any data masking applied
-- MAGIC • Focus on the column, ```credit_card_number```

-- COMMAND ----------

SELECT * 
FROM dim_customers 
LIMIT 10; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Understand the business rules and security requirements

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4 Main User-Group Containers for Permissions Management:
-- MAGIC #### • Data Administrators (DA): can see credit card number unmasked
-- MAGIC ---
-- MAGIC #### • Data Engineers (DE): must see credit card number masked
-- MAGIC #### • Data Scientists (DS): must see credit card number masked
-- MAGIC #### • Data Champions (DC): must see credit card number masked

-- COMMAND ----------

SHOW GROUPS LIKE "D*";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Create the dynamic view with column-level permissions
-- MAGIC ##### Only those that belong to the Data Admins group are able to see the credit card information

-- COMMAND ----------

CREATE VIEW IF NOT EXISTS vw_dim_customers_masked AS 
    SELECT 
        customer_id,
        customer_name,
        city,
        state,
        zip_code,
        brand,
        CASE WHEN 
          is_member("DA_DATA_ADMINS") THEN credit_card_number -- this is where the magic happens -- 
        ELSE "******"
        END AS credit_card_number 
    FROM dim_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Query the view to check if the security rules have been applied
-- MAGIC • The person querying the table below is a Data Champion (DC)

-- COMMAND ----------

SELECT * 
FROM vw_dim_customers_masked

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 6: Even with BI Tools such as Tableau, the masking rules should still work

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://mchanstorage2.blob.core.windows.net/mchan-images/TBL_DATA_MASKING_CLS.png" width="1000"> 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## -- END OF DEMO -- 
