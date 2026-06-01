# Databricks notebook source
# Step-1 - Read the bronze inventory_raw table
# 
df = spark.readStream.table("retails.bronze.inventory_raw")

# COMMAND ----------

# Step-2: Drop raw/debug columns
# Keep _rescued_data only in Bronze for debugging
# 
df = df.drop("_rescued_data", "raw_data")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, current_date

# Step-3: Enforce schema (fix types)
# Even if Bronze inferred schema, re-apply clean schema in Silver
# 
df = df.select(
    col("last_updated").cast("date"),
    col("product_id").cast("string"),
    col("stock_qty").cast("int"),
    col("store_id").cast("string"),
    col("is_deleted").cast("boolean"),
    col("ingestion_ts").cast("timestamp"),
    col("source_system").cast("string")
)


# COMMAND ----------

# Step-4: Define data quality rules (once)
# 
invalid_conditions = (
    col("product_id").isNull() |
    col("store_id").isNull() |
    col("stock_qty").isNull() |
    col("store_id").isNull() |  
    (
        (col("stock_qty").isNotNull()) &
        (col("stock_qty") < 0 )
    )
    
)

# COMMAND ----------

# Step-5: Split Data into valid and invalid
# 
invalid_df = df.filter(invalid_conditions)
valid_df = df.filter(~invalid_conditions)


# COMMAND ----------

# Step-6: Deduplicate (keep latest)
# 
# For now to keep run our pipeline we are just using watermark drop duplicates as below.
# 
valid_df = valid_df.withWatermark("ingestion_ts", "1 day") \
        .dropDuplicates(["product_id", "store_id"])

# We will fix it later using upsert/merge in silver layer

# COMMAND ----------

# Step-7: Handle CDC if applicable
# I am commenting it out because its too early to ignore CDC deleted records.
# we need to implement CDC in down stream(silver layer) with merge/upsert
# 
# valid_df = valid_df.filter(col("is_deleted") == False)

# COMMAND ----------

from pyspark.sql.functions import trim, upper, current_timestamp, current_date

# Step-8: Standardize data
# 
invalid_df = invalid_df.withColumn("ingestion_ts", current_timestamp()) \
                    .withColumn("ingestion_dt", current_date())

valid_df = valid_df.withColumn("product_id", trim(upper(col("product_id")))) \
                    .withColumn("store_id", trim(upper(col("store_id")))) \
                    .withColumn("ingestion_ts", current_timestamp()) \
                    .withColumn("ingestion_dt", current_date())


# COMMAND ----------

# Step-9: Write invalid records (rejected table)
# 
invalid_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/inventory_rejected/") \
    .option("mergeSchema", "true") \
    .partitionBy("ingestion_dt") \
    .trigger(availableNow=True) \
    .table("retails.silver.inventory_rejected")

# COMMAND ----------

# Step-10: Write valid records (final Silver table
# 
valid_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/inventory_cleaned/") \
    .option("mergeSchema", "true") \
    .partitionBy("ingestion_dt") \
    .trigger(availableNow=True) \
    .table("retails.silver.inventory_cleaned")

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.table("retails.bronze.inventory_raw").count()

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.table("retails.silver.inventory_cleaned").count()
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.table("retails.silver.inventory_rejected").count()

# COMMAND ----------

# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/inventory_rejected/", True)
# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/inventory_cleaned/", True)

# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/inventory_cleaned")
# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/inventory_rejected")



# COMMAND ----------

# MAGIC %sql
# MAGIC -- describe table extended retails.silver.inventory_rejected
# MAGIC -- drop table retails.silver.inventory_rejected;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from retails.silver.inventory_cleaned;

# COMMAND ----------

