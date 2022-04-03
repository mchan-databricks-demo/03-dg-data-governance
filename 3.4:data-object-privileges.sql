-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks SQL Object Privileges

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC | Object | Scope |
-- MAGIC | --- | --- |
-- MAGIC | CATALOG | controls access to the entire data catalog. |
-- MAGIC | DATABASE | controls access to a database. |
-- MAGIC | TABLE | controls access to a managed or external table. |
-- MAGIC | VIEW | controls access to SQL views. |
-- MAGIC | FUNCTION | controls access to a named function. |
-- MAGIC | ANY FILE | controls access to the underlying filesystem. Users granted access to ANY FILE can bypass the restrictions put on the catalog, databases, tables, and views by reading from the file system directly. |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://mchanstorage2.blob.core.windows.net/mchan-images/Screenshot 2022-01-30 at 10.37.30 PM.png" width="600"> 

-- COMMAND ----------

-- MAGIC 
-- MAGIC 
-- MAGIC %md
-- MAGIC <img src="https://mchanstorage2.blob.core.windows.net/mchan-images/dbsql_logistics.png" width="1200"> 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: List all the tables in the Database

-- COMMAND ----------

USE mchan_logistics; 
SHOW TABLES FROM mchan_logistics; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: List all of your Databricks users

-- COMMAND ----------

SHOW USERS; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: List all the group containers

-- COMMAND ----------

--If a principal is provided using WITH {USER | GROUP}, a not null Boolean value in column directGroup indicates the principal’s membership.
SHOW GROUPS LIKE "D*";

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Step 4: List the members of the Data Champions (DCs) group 

-- COMMAND ----------

SHOW GROUPS WITH USER `david.mclaughlin@databricks.com`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Grant read-only access to the Data Champions (business end-users)

-- COMMAND ----------

USE mchan_logistics; 

-- USAGE -- 
GRANT USAGE ON DATABASE mchan_logistics TO `DC_DATA_CHAMPIONS`; 

-- VIEW OF THE CONSOLIDATED FACT TABLE -- 
GRANT SELECT on VIEW vw_fct_shipments TO `DC_DATA_CHAMPIONS`; 
-- DIM TABLES -- 
GRANT SELECT on TABLE dim_date TO `DC_DATA_CHAMPIONS`; 
GRANT SELECT on TABLE dim_geography TO `DC_DATA_CHAMPIONS`; 
GRANT SELECT on TABLE dim_product TO `DC_DATA_CHAMPIONS`; 

-- COMMAND ----------

SHOW GRANT `DC_DATA_CHAMPIONS` ON DATABASE mchan_logistics;

-- COMMAND ----------

-- Example showing what the Data Champions can do on the dim_geography table 
SHOW GRANT `DC_DATA_CHAMPIONS` ON TABLE dim_geography; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Grant CREATE, SELECT, MODIFY access to the Data Engineers

-- COMMAND ----------

USE mchan_logistics; 

-- VIEW OF THE CONSOLIDATED FACT TABLE -- 
GRANT SELECT, 
      CREATE, 
      MODIFY 
  ON DATABASE mchan_logistics 
  TO `DE_DATA_ENGINEERS`; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 6: Compare the power of the different groups on the database-level

-- COMMAND ----------

SHOW GRANT 
ON DATABASE mchan_logistics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 6: Compare the power of the different groups on the table-level

-- COMMAND ----------

SHOW GRANT 
  ON VIEW vw_fct_shipments; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 7 (Optional Step): Revoke all privileges from the DCs

-- COMMAND ----------

REVOKE ALL PRIVILEGES 
  ON DATABASE mchan_logistics 
 FROM `DC_DATA_CHAMPIONS`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## -- END OF DEMO -- 
