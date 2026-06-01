# Databricks notebook source
from pyspark.sql.functions import col, expr

# COMMAND ----------

# Step-1: Read from Bronze (Streaming)
# 
df = spark.readStream.table("retails.bronze.customers_raw")

# COMMAND ----------

# Step-2: Drop raw/debug columns
# Keep _rescued_data only in Bronze for debugging
# 
df = df.drop("raw_data", "_rescued_data")

# COMMAND ----------

# Step-3: Enforce schema (fix types)
# Even if Bronze inferred schema, re-apply clean schema in Silver
# 
df = df.withColumn("loyalty_points_int", expr("try_cast(loyalty_points as int)")) \
    .select(
        col("customer_id").cast("string"),
        col("name").cast("string"),
        col("phone").cast("string"),
        col("loyalty_points_int").alias("loyalty_points"), 
        col("is_deleted").cast("boolean"),
        col("ingestion_ts").cast("timestamp"),
        col("ingestion_dt").cast("date"),
        col("source_system"),
        col("record_id")
    )

# COMMAND ----------

# Step-4: Define data quality rules (once)
# 
invalid_condition = (
    col("customer_id").isNull() |
    col("name").isNull() |
    col("phone").isNull() |
    (
        col("loyalty_points").isNotNull() & 
        col("loyalty_points").isNull()
    )
)


# COMMAND ----------

# Step-5: Split Data into valid and invalid
# 
invalid_df = df.filter(invalid_condition)
valid_df = df.filter(~invalid_condition)  

# COMMAND ----------

# Step-6: Deduplicate (keep latest)
#
# This logic is not working in streaming, works fine in batch only so we are not using it as of now
# -------------------------------------------------------------------------------------------------- 
# from pyspark.sql.window import Window
# from pyspark.sql.functions import col, row_number

# window = Window.partitionBy("customer_id").orderBy(col("ingestion_ts").desc())
# valid_df = valid_df.withColumn("rn", row_number().over(window)) \
#                     .filter("rn = 1") \
#                     .drop("rn")
# --------------------------------------------------------------------------------------------------

# For now to keep run our pipeline we are just using watermark drop duplicates as below.
valid_df = valid_df.withWatermark("ingestion_ts", "1 day") \
                    .dropDuplicates(["customer_id"])

# We will fix it later using upsert/merge in silver layer


# COMMAND ----------

# Step-7: Handle CDC if applicable
# I am commenting it out because its too early to ignore CDC deleted records.
# we need to implement CDC in down stream(silver layer) with merge/upsert
# 
# valid_df = valid_df.filter(col("is_deleted") == False)


# COMMAND ----------

# Step-8: Standardize data
# 
from pyspark.sql.functions import trim, upper, current_timestamp, current_date

invalid_df = invalid_df.withColumn("ingestion_ts", current_timestamp()) \
                    .withColumn("ingestion_dt", current_date()) 

valid_df = valid_df.withColumn("name", trim(upper(col("name")))) \
                    .withColumn("phone", trim(col("phone"))) \
                    .withColumn("customer_id", trim(col("customer_id"))) \
                    .withColumn("ingestion_ts", current_timestamp()) \
                    .withColumn("ingestion_dt", current_date())

# COMMAND ----------

# Step-9: Write invalid records (rejected table)
# 
invalid_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/customers_rejected/") \
    .option("mergeSchema", "true") \
    .partitionBy("ingestion_dt") \
    .trigger(availableNow=True) \
    .table("retails.silver.customers_rejected")

# COMMAND ----------

# Step-10: Write valid records (final Silver table)
# 
valid_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/customers_cleaned/") \
    .option("mergeSchema", "true") \
    .partitionBy("ingestion_dt") \
    .trigger(availableNow=True) \
    .table("retails.silver.customers_cleaned")


# COMMAND ----------

# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/customers_rejected/", True)
# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/customers_cleaned/", True)

# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/customers_cleaned")
# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/customers_rejected")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended retails.silver.customers_rejected
# MAGIC -- drop table retails.silver.customers_rejected;

# COMMAND ----------

