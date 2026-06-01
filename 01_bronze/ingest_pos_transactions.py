# Databricks notebook source
from pyspark.sql.functions import current_date, current_timestamp, expr, regexp_extract, col, when, lit, to_json, struct

df = (spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "dbfs:/Volumes/data/raw/_schemas/retails/dev/pos_transactions/") \
    .option("columnNameOfCorruptRecord", "_rescued_data") \
    .load("dbfs:/Volumes/data/raw/sales/raw_data/retail/dev/pos_transactions/")
    )

df = df.withColumnRenamed("date", "txn_date") \
    .withColumnRenamed("hour", "txn_hour") \
    .withColumnRenamed("txn_time", "txn_timestamp")

df = df.withColumn("raw_data", to_json(struct("*")))

if "op" in df.columns:
    df = df.withColumn("is_deleted",
                       when(col("op") == "DELETE", True).otherwise(False))
else:
    df = df.withColumn("is_deleted", lit(False))

df = (
    df.
        withColumn("ingestion_ts", current_timestamp()) \
        .withColumn("ingestion_dt", current_date()) \
        .withColumn("source_file_name", regexp_extract(col("_metadata.file_path"), r'([^/]+$)', 1)) \
        .withColumn("record_id", expr("uuid()")) \
        .withColumn("source_system", lit("POS")) \
)
(df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "dbfs:/Volumes/data/raw/_checkpoints/retails/dev/bronze/pos_transactions/") \
    .option("mode", "append") \
    .option("mergeSchema", "true") \
    .option("path", "s3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/retails/pos_transactions_raw") \
    .partitionBy("txn_date") \
    .trigger(availableNow=True) \
    .toTable("retails.bronze.pos_transactions_raw")
)

# COMMAND ----------

GRANTS = "ALTER TABLE retails.bronze.pos_transactions_raw OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

