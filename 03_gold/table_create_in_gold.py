# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE retails.gold.dim_products (
# MAGIC
# MAGIC     product_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC
# MAGIC     product_id BIGINT,
# MAGIC     product_name STRING,
# MAGIC     product_description STRING,
# MAGIC     product_price DOUBLE,
# MAGIC     product_image STRING,
# MAGIC     product_category_id BIGINT,
# MAGIC
# MAGIC     is_active BOOLEAN,
# MAGIC
# MAGIC     effective_from TIMESTAMP,
# MAGIC     effective_to TIMESTAMP,
# MAGIC     is_current BOOLEAN,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.gold.dim_departments (
# MAGIC
# MAGIC     department_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC
# MAGIC     department_id INT,
# MAGIC     department_name STRING,
# MAGIC
# MAGIC     is_current BOOLEAN,
# MAGIC     effective_from TIMESTAMP,
# MAGIC     effective_to TIMESTAMP,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.gold.dim_categories (
# MAGIC
# MAGIC     category_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC
# MAGIC     category_id INT,
# MAGIC     category_department_id INT,
# MAGIC     category_name STRING,
# MAGIC
# MAGIC     is_current BOOLEAN,
# MAGIC     effective_from TIMESTAMP,
# MAGIC     effective_to TIMESTAMP,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.gold.dim_customers (
# MAGIC
# MAGIC     customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC
# MAGIC     customer_id INT,
# MAGIC
# MAGIC     customer_fname STRING,
# MAGIC     customer_lname STRING,
# MAGIC     customer_email STRING,
# MAGIC
# MAGIC     customer_street STRING,
# MAGIC     customer_city STRING,
# MAGIC     customer_state STRING,
# MAGIC     customer_zipcode INT,
# MAGIC
# MAGIC     is_current BOOLEAN,
# MAGIC     effective_from TIMESTAMP,
# MAGIC     effective_to TIMESTAMP,
# MAGIC
# MAGIC     created_ts TIMESTAMP,
# MAGIC     updated_ts TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.gold.dim_dates (
# MAGIC
# MAGIC     date_key INT,
# MAGIC
# MAGIC     full_date DATE,
# MAGIC     year INT,
# MAGIC     quarter INT,
# MAGIC     month INT,
# MAGIC     month_name STRING,
# MAGIC     week_of_year INT,
# MAGIC     day_of_month INT,
# MAGIC     day_of_week INT,
# MAGIC     day_name STRING,
# MAGIC     is_weekend BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.gold.fact_orders (
# MAGIC
# MAGIC     order_fact_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC
# MAGIC     order_id INT,
# MAGIC
# MAGIC     customer_key BIGINT,
# MAGIC     order_date_key INT,
# MAGIC
# MAGIC     order_status STRING,
# MAGIC
# MAGIC     total_order_amount DOUBLE,
# MAGIC     total_items INT,
# MAGIC
# MAGIC     created_ts TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retails.gold.fact_order_items (
# MAGIC
# MAGIC     order_item_fact_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC
# MAGIC     order_item_id INT,
# MAGIC     order_id INT,
# MAGIC
# MAGIC     customer_key BIGINT,
# MAGIC     product_key BIGINT,
# MAGIC     category_key BIGINT,
# MAGIC     department_key BIGINT,
# MAGIC
# MAGIC     order_date_key INT,
# MAGIC
# MAGIC     quantity INT,
# MAGIC     product_price DOUBLE,
# MAGIC     subtotal DOUBLE,
# MAGIC
# MAGIC     created_ts TIMESTAMP
# MAGIC );

# COMMAND ----------

