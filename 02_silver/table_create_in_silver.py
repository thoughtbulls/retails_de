# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.silver.departments_cleaned (
# MAGIC
# MAGIC     department_id BIGINT,
# MAGIC     department_name STRING,
# MAGIC
# MAGIC     op STRING,
# MAGIC     is_deleted BOOLEAN,
# MAGIC
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     batch_id BIGINT,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     record_hash STRING,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.silver.departments_quarantine (
# MAGIC
# MAGIC     quarantine_id STRING,
# MAGIC
# MAGIC     department_id STRING,
# MAGIC     department_name STRING,
# MAGIC
# MAGIC     op STRING,
# MAGIC
# MAGIC     _rescued_data STRING,
# MAGIC     _corrupt_record STRING,
# MAGIC
# MAGIC     quarantine_reason STRING,
# MAGIC     quarantine_status STRING,
# MAGIC
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     batch_id BIGINT,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.silver.categories_cleaned (
# MAGIC
# MAGIC     category_id BIGINT,
# MAGIC     category_department_id BIGINT,
# MAGIC     category_name STRING,
# MAGIC
# MAGIC     op STRING,
# MAGIC     is_deleted BOOLEAN,
# MAGIC
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     batch_id BIGINT,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     record_hash STRING,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.silver.categories_quarantine (
# MAGIC
# MAGIC     quarantine_id STRING,
# MAGIC
# MAGIC     category_id STRING,
# MAGIC     category_department_id STRING,
# MAGIC     category_name STRING,
# MAGIC
# MAGIC     op STRING,
# MAGIC
# MAGIC     _rescued_data STRING,
# MAGIC     _corrupt_record STRING,
# MAGIC
# MAGIC     quarantine_reason STRING,
# MAGIC     quarantine_status STRING,
# MAGIC
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     batch_id BIGINT,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retails.silver.products_cdc (
# MAGIC
# MAGIC     -- Business Columns
# MAGIC     product_id BIGINT,
# MAGIC     product_name STRING,
# MAGIC     product_description STRING,
# MAGIC     product_price DECIMAL(10,2),
# MAGIC     product_image STRING,
# MAGIC     product_category_id BIGINT,
# MAGIC
# MAGIC     -- CDC Event Metadata
# MAGIC     op STRING,                     -- INSERT / UPDATE / DELETE
# MAGIC     event_ts TIMESTAMP,
# MAGIC     is_deleted BOOLEAN,
# MAGIC
# MAGIC     -- CDC Tracking
# MAGIC     -- change_version BIGINT,
# MAGIC     -- change_type STRING,
# MAGIC
# MAGIC     -- Source Metadata
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     -- Pipeline Metadata
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC     batch_id INTEGER,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     -- Optional Tracking
# MAGIC     record_hash STRING
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retails.silver.products_cleaned (
# MAGIC
# MAGIC     -- Business Columns
# MAGIC     product_id BIGINT,
# MAGIC     product_name STRING,
# MAGIC     product_description STRING,
# MAGIC     product_price DECIMAL(10,2),
# MAGIC     product_image STRING,
# MAGIC     product_category_id BIGINT,
# MAGIC
# MAGIC     -- CDC State
# MAGIC     is_deleted BOOLEAN,
# MAGIC
# MAGIC     -- Source Metadata
# MAGIC     op STRING,                  -- INSERT / UPDATE / DELETE
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC     
# MAGIC     -- Pipeline Metadata
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC     batch_id INTEGER,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     -- Audit Columns
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP,
# MAGIC
# MAGIC     -- Optional Tracking
# MAGIC     record_hash STRING
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.silver.products_quarantine (
# MAGIC
# MAGIC     -- Auto generated PK
# MAGIC     quarantine_id STRING,
# MAGIC
# MAGIC     -- Business Columns
# MAGIC     product_id STRING,
# MAGIC     product_name STRING,
# MAGIC     product_description STRING,
# MAGIC     product_price STRING,
# MAGIC     product_image STRING,
# MAGIC     product_category_id STRING,
# MAGIC
# MAGIC     -- Source Metadata
# MAGIC     op STRING,                  -- INSERT / UPDATE / DELETE
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     -- Quarantine columns
# MAGIC     _rescued_data STRING,
# MAGIC     _corrupt_record STRING,
# MAGIC     quarantine_reason STRING,
# MAGIC     quarantine_status STRING,
# MAGIC
# MAGIC     -- Pipeline Metadata
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC     batch_id BIGINT,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     -- Audit Columns
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.silver.customers_cleaned (
# MAGIC
# MAGIC     customer_id BIGINT,
# MAGIC
# MAGIC     customer_fname STRING,
# MAGIC     customer_lname STRING,
# MAGIC
# MAGIC     customer_email STRING,
# MAGIC     customer_password STRING,
# MAGIC
# MAGIC     customer_street STRING,
# MAGIC     customer_city STRING,
# MAGIC     customer_state STRING,
# MAGIC
# MAGIC     customer_zipcode BIGINT,
# MAGIC
# MAGIC     op STRING,
# MAGIC     is_deleted BOOLEAN,
# MAGIC
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     batch_id BIGINT,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     record_hash STRING,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.silver.customers_quarantine (
# MAGIC
# MAGIC     quarantine_id STRING,
# MAGIC
# MAGIC     customer_id STRING,
# MAGIC
# MAGIC     customer_fname STRING,
# MAGIC     customer_lname STRING,
# MAGIC
# MAGIC     customer_email STRING,
# MAGIC     customer_password STRING,
# MAGIC
# MAGIC     customer_street STRING,
# MAGIC     customer_city STRING,
# MAGIC     customer_state STRING,
# MAGIC
# MAGIC     customer_zipcode STRING,
# MAGIC
# MAGIC     op STRING,
# MAGIC
# MAGIC     _rescued_data STRING,
# MAGIC     _corrupt_record STRING,
# MAGIC
# MAGIC     quarantine_reason STRING,
# MAGIC     quarantine_status STRING,
# MAGIC
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     batch_id BIGINT,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.silver.orders_cleaned (
# MAGIC
# MAGIC     order_id BIGINT,
# MAGIC
# MAGIC     order_date DATE,
# MAGIC     order_customer_id BIGINT,
# MAGIC
# MAGIC     order_status STRING,
# MAGIC
# MAGIC     op STRING,
# MAGIC     is_deleted BOOLEAN,
# MAGIC
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     batch_id BIGINT,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     record_hash STRING,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.silver.orders_quarantine (
# MAGIC
# MAGIC     quarantine_id STRING,
# MAGIC
# MAGIC     order_id STRING,
# MAGIC     order_date STRING,
# MAGIC
# MAGIC     order_customer_id STRING,
# MAGIC     order_status STRING,
# MAGIC
# MAGIC     op STRING,
# MAGIC
# MAGIC     _rescued_data STRING,
# MAGIC     _corrupt_record STRING,
# MAGIC
# MAGIC     quarantine_reason STRING,
# MAGIC     quarantine_status STRING,
# MAGIC
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     batch_id BIGINT,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.silver.order_items_cleaned (
# MAGIC
# MAGIC     order_item_id BIGINT,
# MAGIC     order_item_order_id BIGINT,
# MAGIC     order_item_product_id BIGINT,
# MAGIC
# MAGIC     order_item_quantity BIGINT,
# MAGIC
# MAGIC     order_item_subtotal DECIMAL(10,2),
# MAGIC     order_item_product_price DECIMAL(10,2),
# MAGIC
# MAGIC     op STRING,
# MAGIC     is_deleted BOOLEAN,
# MAGIC
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     batch_id BIGINT,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     record_hash STRING,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.silver.order_items_quarantine (
# MAGIC
# MAGIC     quarantine_id STRING,
# MAGIC
# MAGIC     order_item_id STRING,
# MAGIC     order_item_order_id STRING,
# MAGIC     order_item_product_id STRING,
# MAGIC
# MAGIC     order_item_quantity STRING,
# MAGIC
# MAGIC     order_item_subtotal STRING,
# MAGIC     order_item_product_price STRING,
# MAGIC
# MAGIC     op STRING,
# MAGIC
# MAGIC     _rescued_data STRING,
# MAGIC     _corrupt_record STRING,
# MAGIC
# MAGIC     quarantine_reason STRING,
# MAGIC     quarantine_status STRING,
# MAGIC
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     batch_id BIGINT,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.silver.cdc_rejected_events (
# MAGIC
# MAGIC     reject_id STRING,
# MAGIC
# MAGIC     entity_name STRING,
# MAGIC     business_key STRING,
# MAGIC
# MAGIC     op STRING,
# MAGIC
# MAGIC     reject_reason STRING,
# MAGIC
# MAGIC     payload STRING,
# MAGIC
# MAGIC     ingestion_ts TIMESTAMP,
# MAGIC     ingestion_dt DATE,
# MAGIC
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING,
# MAGIC
# MAGIC     batch_id BIGINT,
# MAGIC     run_id STRING,
# MAGIC
# MAGIC     created_ts TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.products_cdc OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in retails.silver;

# COMMAND ----------

