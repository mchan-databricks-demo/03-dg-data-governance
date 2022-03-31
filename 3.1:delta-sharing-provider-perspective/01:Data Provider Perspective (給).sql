-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Sharing: Data Provider's Perspective
-- MAGIC ### • Data Provider: Travel Booking Agency
-- MAGIC ### • Data Recipient: Airline Companies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://mchanstorage2.blob.core.windows.net/mchan-images/Screenshot 2022-01-27 at 2.37.03 PM.png" width="1000"> 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Unity Catalog will be your Entitlement Layer Managed by Databricks
-- MAGIC As a data provider, you can make your Unity Catalog Metastore act as a Delta Sharing Server and share data on Unity Catalog with other organizations.  
-- MAGIC These organizations can then access the data using open source Apache Spark or pandas on any computing platform (including, but not limited to, Databricks). <br> <br>
-- MAGIC # <img src="https://i.ibb.co/QJny676/Screen-Shot-2021-11-16-at-10-46-49-AM.png" width="600" height="480" /><br>
-- MAGIC                                                                                                 

-- COMMAND ----------

--REMOVE WIDGET SHARE_1;
--REMOVE WIDGET SHARE_2;
--REMOVE WIDGET RECIPIENT_1;
--REMOVE WIDGET RECIPIENT_2;
--CREATE WIDGET TEXT SHARE_1 DEFAULT "";
--CREATE WIDGET TEXT  SHARE_2 DEFAULT "";
--CREATE WIDGET TEXT RECIPIENT_1 DEFAULT "";
--CREATE WIDGET TEXT RECIPIENT_2 DEFAULT "";

-- COMMAND ----------

-- RUN THIS CELL PRIOR TO DEMO For Re-Execution 
-- make sure you are using a delta sharing enabled workspace & a UC enabled Cluster
DROP RECIPIENT IF EXISTS americanairlines;
DROP RECIPIENT IF EXISTS southwestairlines;
DROP SHARE IF EXISTS americanairlines;
DROP SHARE IF EXISTS southwestairlines;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Step 1: Create the Delta Sharing Catalog
-- MAGIC #### • Permissions model is based on ANSI-SQL GRANT control language
-- MAGIC #### • Use case: Create 2 shares to share data to 2 partner airlines
-- MAGIC #### • Security requirement: A particular airline should be able to see their own data only

-- COMMAND ----------

-- CATALOG is also 
USE CATALOG deltasharing; 

-- COMMAND ----------

CREATE SHARE IF NOT EXISTS americanairlines 
  COMMENT 'Daily Flight Data provided by Tripactions to American Airlines only';

CREATE SHARE IF NOT EXISTS southwestairlines 
    COMMENT 'Daily Flight Data provided by Tripactions to Southwest Airlines only';

-- COMMAND ----------

-- DBTITLE 0,View a Share’s Metadata
DESCRIBE SHARE americanairlines;
DESCRIBE SHARE southwestairlines

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Step 2: Add Tables to the Share
-- MAGIC #### • A share is like a box where you put the relevant tables inside for your recipients to access

-- COMMAND ----------

ALTER SHARE americanairlines 
    ADD TABLE airlinedata.lookupcodes;
    
ALTER SHARE southwestairlines 
    ADD TABLE airlinedata.lookupcodes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Undo mistakes with the ```REMOVE``` clause

-- COMMAND ----------

ALTER SHARE americanairlines 
    REMOVE TABLE airlinedata.lookupcodes;
    
ALTER SHARE southwestairlines 
    REMOVE TABLE airlinedata.lookupcodes; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Step 3: Implement the Security Filter
-- MAGIC #### • We don't need to give all historical flights to each airline
-- MAGIC #### • This is implemented via a ```PARTITION``` filter

-- COMMAND ----------

-- DBTITLE 0,Step 3: Add Partition Filter to Only Share a Portion of the Flights Table Using "=" & LIKE Operators AND Customized Aliases
ALTER SHARE americanairlines 
    ADD TABLE deltasharing.airlinedata.flights 
    PARTITION (UniqueCarrier = "AA")

-- COMMAND ----------

ALTER SHARE southwestairlines 
    ADD TABLE deltasharing.airlinedata.flights 
    PARTITION (UniqueCarrier = "WN")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Step 4: Create a Data Recipient who can access your data

-- COMMAND ----------

CREATE RECIPIENT southwestairlines;
CREATE RECIPIENT americanairlines; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Step 5: Your Data Recipient will receive an e-mail activation URL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/kanonymity_share_activation.png" width=650>

-- COMMAND ----------

-- DBTITLE 0,Download Share Profile & Store on DBFS - American Airlines
-- MAGIC %python
-- MAGIC 
-- MAGIC import urllib.request
-- MAGIC sql("""DROP RECIPIENT IF EXISTS americanairlines""")
-- MAGIC df = sql("""CREATE RECIPIENT americanairlines""")
-- MAGIC link = df.collect()[0][4].replace('delta_sharing/retrieve_config.html?','api/2.0/unity-catalog/public/data_sharing_activation/')
-- MAGIC urllib.request.urlretrieve(link, "/tmp/americanairlines.share")
-- MAGIC dbutils.fs.mv("file:/tmp/americanairlines.share", "dbfs:/FileStore/americanairlines.share")

-- COMMAND ----------

-- DBTITLE 0,Download Share Profile & Store on DBFS - Southwest Airlines
-- MAGIC %python
-- MAGIC 
-- MAGIC import urllib.request
-- MAGIC df = sql("""DESCRIBE RECIPIENT southwestairlines""")
-- MAGIC link = df.collect()[0][4].replace('delta_sharing/retrieve_config.html?','api/2.0/unity-catalog/public/data_sharing_activation/')
-- MAGIC urllib.request.urlretrieve(link, "/tmp/southwestairlines.share")
-- MAGIC dbutils.fs.mv("file:/tmp/southwestairlines.share", "dbfs:/FileStore/southwestairlines.share")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Step 6: Define the level of access to your data 

-- COMMAND ----------

--'SELECT' gives read only permissions on all tables in the share
GRANT SELECT ON SHARE americanairlines 
    TO RECIPIENT americanairlines;
    
GRANT SELECT ON SHARE southwestairlines 
    TO RECIPIENT southwestairlines; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Step 7: Check what your Data Recepient can do to your share

-- COMMAND ----------

SHOW GRANT ON SHARE americanairlines;

-- COMMAND ----------

-- DBTITLE 0,Step 10: Audit recipients
DESCRIBE RECIPIENT americanairlines;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Step 8: Revoke Access if Necessary

-- COMMAND ----------

REVOKE SELECT ON SHARE southwestairlines 
    FROM RECIPIENT southwestairlines; 

-- COMMAND ----------

SHOW ALL IN SHARE southwestairlines;
SHOW ALL IN SHARE americanairlines

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # -- END OF DEMO -- 
