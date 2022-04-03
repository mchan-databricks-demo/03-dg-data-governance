-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Databricks SQL: Row-Level Security Demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Databricks includes two user functions that allow you to express column- and row-level permissions dynamically in the body of a view definition.
-- MAGIC 
-- MAGIC ##### 1. ```current_user()```: return the current user name.
-- MAGIC 
-- MAGIC ##### 2. ```is_member()```: determine if the current user is a member of a specific Databricks group.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Implementing RLS involves the following components: 
-- MAGIC ##### Component 1. Your main ```FACT``` table in which RLS will be applied
-- MAGIC ##### Component 2. A ```DIM``` table with the business rules for permissions
-- MAGIC ##### Component 3. A Dynamic ```VIEW``` joining the 2 tables together, using the ```current_user()``` function

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Examine the data without RLS applied 

-- COMMAND ----------

-- set the database context 
USE mchan_row_level_security; 

SELECT * 
FROM vw_apac_orders 
ORDER BY 3; 

-- COMMAND ----------

SELECT DISTINCT order_country, 
                country_name, 
                sub_region, 
                super_region 
FROM vw_apac_orders
ORDER BY 3,2; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Understand the Row-Level Security (RLS) requirements of the Business
-- MAGIC ##### • Req-1. Country-Level access: can only query data from one country
-- MAGIC ----
-- MAGIC ##### • Req-2. Sub Region-Level access: can query data from multiple countries as long as it rolls up to the sub-region
-- MAGIC ###### *East Asia = China, Japan, Korea*
-- MAGIC ######  *Southeast Asia = Philippines, Singapore, Vietnam*
-- MAGIC ---
-- MAGIC ##### • Req-3. Region-Level access: users have no restrictions and may see all the data

-- COMMAND ----------

SELECT * 
FROM dim_security_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Create the Dynamic View with RLS Embedded Inside

-- COMMAND ----------

CREATE OR REPLACE VIEW vw_apac_orders_secure 
AS 
  SELECT * 
  FROM vw_apac_orders t1 
  INNER JOIN dim_security_table t2 
  ON current_user() = t2.username -- this is the secret sauce -- 
  WHERE 
      t2.restriction_level = "Region"
  OR (
      -- Sub-Region and Country level Restriction -- 
      CASE t2.restriction_level
        WHEN "Sub Region" THEN t1.sub_region 
        WHEN "Country"  THEN t1.country_code 
      END = t2.data_restriction_level 
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Check and see if RLS has been applied
-- MAGIC ##### • Notice how only orders from Southeast Asia are appearing 
-- MAGIC ##### • Since Mendelsohn Chan has ```data_restriction_level``` = "Southeast Asia" applied

-- COMMAND ----------

-- This is a dynamic function that updates based on the person querying
SELECT CURRENT_USER();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### IMPORTANT! Notice how only records from Southeast Asia are showing 

-- COMMAND ----------

-- Query Result as Username: mendelsohn.chan@databricks.com
SELECT order_id, 
       order_country,
       country_name,
       sub_region
FROM vw_apac_orders_secure;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Revoke access to the view, ```vw_apac_orders``` without RLS applied
-- MAGIC #### • ```DENY``` access to the unguarded view 
-- MAGIC #### • ```GRANT``` access to the view with RLS logic applied
-- MAGIC #### • ```DC_DATA_CHAMPIONS``` is a logical grouping with end-users contained inside it

-- COMMAND ----------

-- deny on the original view 
-- this is not the table that end-users should query
DENY SELECT ON VIEW vw_apac_orders 
    TO `DC_DATA_CHAMPIONS`; 

-- grant read-only access to the secure view
-- this is the table that end-users should query 
GRANT SELECT ON VIEW vw_apac_orders_secure
    TO `DC_DATA_CHAMPIONS`; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## -- END OF DEMO -- 
