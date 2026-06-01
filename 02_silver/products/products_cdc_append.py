# Databricks notebook source
from pyspark.sql.functions import to_date, current_date, col

# Step-1: Read from Bronze (Streaming)
# 
df = spark.readStream.table("retails.bronze.products_raw")


# COMMAND ----------

from pyspark.sql.functions import col

# Step 2: Separate Good vs Bad Records 
valid_condition = (
    (col("_rescued_data").isNull()) &
    (col("_corrupt_record").isNull())
)

invalid_df = df.filter(~valid_condition)
valid_df = df.filter(valid_condition)



# COMMAND ----------

from pyspark.sql.functions import to_json, struct, when, col, sha2, concat_ws

# Step 4: Apply Type Casting & Standardization
valid_df = valid_df.withColumn("product_id", col("product_id").cast("bigint")) \
                    .withColumn("product_category_id", col("product_category_id").cast("bigint")) \
                    .withColumn("product_name", col("product_name").cast(("string"))) \
                    .withColumn("product_price", col("product_price").cast("decimal(10,2)")) \
                    .withColumn("batch_id", col("batch_id").cast("integer")) \
                    .withColumn("is_deleted", when(col("op")=='DELETE', True).otherwise(False))


# COMMAND ----------

# invalid_df.printSchema()

# COMMAND ----------

# Step 5: Data Quality Checks
valid_price_con = (
        col("product_id").isNotNull() &
        (col("product_price") >= 0)
)
quality_valid_df = valid_df.filter(valid_price_con)

invalid_quality_df = valid_df.filter(~valid_price_con)
invalid_df = invalid_df.union(invalid_quality_df.drop("is_deleted"))
# invalid_quality_df.printSchema()


# COMMAND ----------

# Step-3: Drop raw/debug columns
# Keep _rescued_data only in Bronze for debugging
# 
quality_valid_df = quality_valid_df.drop("raw_data") \
        .drop("_rescued_data") \
        .drop("_corrupt_record") \
        .drop("source_file_path") \
        .drop("replay_flag")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, sha2, concat_ws

quality_valid_df = quality_valid_df \
            .withColumn("event_ts", current_timestamp()) \
            .withColumn("record_hash",
                        sha2(
                            concat_ws(
                                "||",
                                col("product_id"),
                                col("product_name"),
                                col("product_description"),
                                col("product_price"),
                                col("product_category_id"),
                                col("product_image")
                            ),
                            256
                        )
                    )

# COMMAND ----------

# Step 6: Deduplication
silver_df = quality_valid_df.dropDuplicates(["record_hash","op", "run_id"])

# Step 7: Remove NA/None/NAN/null records for targeting spasific columns
silver_df = silver_df.dropna(how="any", subset=["product_name", "product_price", "product_category_id"])

# Step 8: Fill 'Unknown' for NA/None/NAN/null for targeting spasific columns
silver_df = silver_df.fillna("Unknown", subset=["product_description"])

# COMMAND ----------

from pyspark.sql.functions import *
import uuid

# Step 7: Quarantine table fill the metadata columns
quarantine_df = (
    invalid_df
  # Generate quarantine ID
    .withColumn("quarantine_id", expr("uuid()"))

    # -----------------------------------------
    # Error Reason
    # -----------------------------------------

    .withColumn(
        "error_reason",
        when(
            col("_corrupt_record").isNotNull(),
            lit("Malformed JSON record")
        ).when(
            col("_rescued_data").isNotNull(),
            lit("Schema drift / unexpected columns")
        )
    )

    # -----------------------------------------
    # Error Category
    # -----------------------------------------

    .withColumn(
        "error_category",
        when(
            col("_corrupt_record").isNotNull(),
            lit("CORRUPT_RECORD")
        ).when(
            col("_rescued_data").isNotNull(),
            lit("SCHEMA_DRIFT")
        )
    )

    # -----------------------------------------
    # Failed Column
    # -----------------------------------------

    .withColumn(
        "failed_column",
        when(
            col("_corrupt_record").isNotNull(),
            lit("FULL_RECORD")
        ).when(
            col("_rescued_data").isNotNull(),
            lit("_rescued_data")
        )
    )

    # -----------------------------------------
    # Validation Rule
    # -----------------------------------------

    .withColumn(
        "validation_rule",
        when(
            col("_corrupt_record").isNotNull(),
            lit("Valid JSON format expected")
        ).when(
            col("_rescued_data").isNotNull(),
            lit("Schema must match expected schema")
        )
    )

    # -----------------------------------------
    # Metadata Columns
    # -----------------------------------------

    .withColumn("quarantine_status", lit("NEW"))

    .withColumn("rejected_at", current_timestamp())

    .withColumn("reprocessed_at", lit(None).cast("timestamp"))

)

quarantine_stream_df = (
    quarantine_df
        .withColumn("product_price", col("product_price").cast("string"))
        .withColumn("batch_id", col("batch_id").cast("string"))
)

# COMMAND ----------

def process_quarantine_batch(batch_df, batch_id):
    # batch_df.createOrReplaceGlobalTempView("batch_df")
    try:
        batch_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "false") \
            .saveAsTable("retails.silver.products_quarantine")
    except Exception as e:
        print(str(e))


# COMMAND ----------

# Step 8: Store Bad Records Separately (Quarantine Table)
try:
    quarantine_stream_df.writeStream \
        .foreachBatch(process_quarantine_batch) \
        .option("checkpointLocation", "dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/products_quarantine") \
        .trigger(availableNow=True) \
        .start()

except Exception as e:
    print(str(e))



# COMMAND ----------

def batch_append_to_cdc(batch_df, batch_id):
    try:
        batch_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "false") \
            .saveAsTable("retails.silver.products_cdc")
    except Exception as e:
        raise e


# COMMAND ----------

#write append data in products_cdc table
try:
    silver_df.writeStream \
        .foreachBatch(batch_append_to_cdc) \
        .option("checkpointLocation", "dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/products_cdc/") \
        .trigger(availableNow=True) \
        .start()
except Exception as e:
    print(str(e))


# COMMAND ----------

# %run ./products_rescued_fix

# COMMAND ----------

# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/bronze/products/")
# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/products_quarantine/")
# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/products_cdc/")
# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/products_cleaned/")
# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/gold/products/")

# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/bronze/products/", True)
# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/products_cdc/", True)
# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/products_quarantine/", True)
# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/products_cleaned/", True)

# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/gold/products/", True)
# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/gold/products_clean/", True)



# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Define the window specification
window_spec = Window.partitionBy("product_id").orderBy(F.col("event_ts").desc())

# 2. Apply row_number() and filter for ranks 1 and 2
result_df = (
    spark.table("retails.silver.products_cdc")
    .withColumn("rank", F.row_number().over(window_spec))
    .filter(F.col("rank") == 2)
)

# 3. View the results
display(result_df)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- describe table retails.silver.products_cleaned;
# MAGIC -- select * from retails.silver.products_quarantine;
# MAGIC -- select * from retails.silver.products_cleaned where product_id=24;
# MAGIC -- select max(run_id), min(run_id) from retails.silver.products_cdc;
# MAGIC -- select * from retails.silver.products_quarantine;
# MAGIC
# MAGIC -- with rank_products as (
# MAGIC -- select product_id, product_name, product_price, 
# MAGIC -- row_number() over(partition by product_id order by run_id desc) as rank
# MAGIC -- from retails.silver.products_cdc
# MAGIC -- )
# MAGIC -- select * from rank_products where rank=1;
# MAGIC
# MAGIC -- select * from retails.bronze.products_raw;
# MAGIC -- select * from retails.silver.products_cdc order by product_id, run_id desc;
# MAGIC -- select * from retails.silver.products_quarantine;
# MAGIC -- select * from retails.silver.products_cleaned order by product_id;
# MAGIC select * from retails.gold.dim_products where product_id between 23 and 24 order by product_id ;
# MAGIC
# MAGIC -- truncate table retails.silver.products_cleaned;
# MAGIC -- truncate table retails.bronze.products_raw;
# MAGIC -- truncate table retails.silver.products_quarantine;
# MAGIC -- truncate table retails.silver.products_cdc;
# MAGIC -- truncate table retails.gold.dim_products;

# COMMAND ----------

# quarantine_status	Meaning
# NEW	Newly quarantined record, not yet reviewed
# INVALID	Permanently invalid record
# RESCUED	Record has rescued_data but may be recoverable
# CORRUPT	Completely corrupt JSON/file structure
# PARTIAL_VALID	Some columns valid, some problematic
# FIXED	Data corrected successfully
# REPROCESSED	Re-ingested into clean table