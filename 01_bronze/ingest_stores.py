# Databricks notebook source
from pyspark.sql.functions import current_date, current_timestamp, expr, regexp_extract, col, when, lit, to_json, struct

df = (spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "dbfs:/Volumes/data/raw/_schemas/retails/dev/stores/") \
    .option("columnNameOfCorruptRecord", "_rescued_data") \
    .load("dbfs:/Volumes/data/raw/sales/raw_data/retail/dev/stores/")
    )

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
        .withColumn("source_system", lit("ERP")) \
)
(df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "dbfs:/Volumes/data/raw/_checkpoints/retails/dev/bronze/stores/") \
    .option("mode", "append") \
    .option("mergeSchema", "true") \
    .option("path", "s3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/retails/stores_raw") \
    .partitionBy("ingestion_dt") \
    .trigger(availableNow=True) \
    .toTable("retails.bronze.stores_raw")
)

# COMMAND ----------

GRANTS = "ALTER TABLE retails.bronze.stores_raw OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

