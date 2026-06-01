# Databricks notebook source
checkpoints = "dbfs:/Volumes/sales/bronze/vol_sales/checkpoints/orders"
schemas = "dbfs:/Volumes/sales/bronze/vol_sales/schemas/orders"
raw_data_orders = "dbfs:/Volumes/data/raw/sales/read_input/orders"
# auto loader read data from volume dbfs:/Volumes/data/raw/orders
orders_df_new = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", schemas)
      .load(raw_data_orders))

orders_df_new.printSchema()

# .option("inferColumnTypes", "true")


# COMMAND ----------

from pyspark.sql.functions import col, to_date, try_to_date
from pyspark.sql.functions import current_timestamp
orders_df_new_columns = orders_df_new \
            .withColumn("order_id", col("order_id").isNotNull()) \
            .withColumn("working_date", col("order_date").isNotNull()) \
            .withColumn("is_valid", col("order_date").isNotNull()) \
            .withColumn("inserted_at", current_timestamp())


orders_df_new_columns = orders_df_new_columns.drop("city")
orders_df_new_columns.printSchema()

# COMMAND ----------

orders_df_new_columns.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", checkpoints) \
    .option("mergeSchema", "true") \
    .toTable("sales.bronze.orders_raw")

    #     .option("mergeSchema", "true") \

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from sales.bronze.orders_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales.bronze.orders_raw 
# MAGIC     where order_date = '2013-08-08'
# MAGIC     -- limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe sales.bronze.orders_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC with duplicate as (
# MAGIC select *, row_number() over(partition by order_id order by _metadata.file_name DESC) as rn from sales.bronze.orders_raw
# MAGIC )
# MAGIC select order_id, order_customer_id, order_status, order_date, city, `_rescued_data` from duplicate where rn = 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table sales.bronze.orders_raw_tmp as
# MAGIC select * from (with duplicate as (
# MAGIC select *, row_number() over(partition by order_id order by _metadata.file_name DESC) as rn from sales.bronze.orders_raw
# MAGIC )
# MAGIC select order_id, order_customer_id, order_status, order_date, city, `_rescued_data` from duplicate where rn = 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table if exists sales.bronze.orders_raw_tmp;
# MAGIC -- select * from sales.bronze.orders_raw_tmp;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- truncate table sales.bronze.orders_raw;
# MAGIC select * from sales.bronze.orders_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into sales.bronze.orders_raw
# MAGIC select * from sales.bronze.orders_raw_tmp order by order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) order_date from sales.bronze.orders_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sales.bronze.orders_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table sales.bronze.orders_raw to version as of 11;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from sales.bronze.orders_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE sales.bronze.orders_raw;

# COMMAND ----------

