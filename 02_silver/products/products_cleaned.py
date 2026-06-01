# Databricks notebook source
from pyspark.sql.functions import to_date, current_date, col

# Step-1: Read from Bronze (Streaming)
# 
# df = spark.read.table("retails.bronze.products_raw") \
#                 .filter(col("product_category_id") == 2)
cdc_df = spark.readStream.table("retails.silver.products_cdc")


# COMMAND ----------

from pyspark.sql.functions import to_json, struct, when, col

# Step 4: Apply Type Casting & Standardization
cdc_df = cdc_df.withColumn("product_id", col("product_id").cast("integer")) \
                    .withColumn("product_category_id", col("product_category_id").cast("integer")) \
                    .withColumn("product_name", col("product_name").cast(("string"))) \
                    .withColumn("product_price", col("product_price").cast("double")) \
                    .withColumn("product_category_id", col("product_category_id").cast("integer")) \
                    .withColumn("record_hash", col("record_hash"))
                        


# COMMAND ----------

# Step 6: Deduplication
# cdc_df = cdc_df.dropDuplicates(["record_hash"])


# COMMAND ----------

# cdc_df.count()

# COMMAND ----------

# make ready to upsert silver table products_cleaned
silver_df = cdc_df \
            .withColumn("product_id", col("product_id").cast("bigint")) \
            .withColumn("product_price", col("product_price").cast("decimal(10,2)")) \
            .withColumn("product_category_id", col("product_category_id").cast("bigint")) \
            .withColumn("batch_id", col("batch_id").cast("string"))


# COMMAND ----------

# upsert products_cleaned table
MERGE_UPDATE = """
    MERGE INTO retails.silver.products_cleaned t
    USING global_temp.products_cleaned_vw s
    ON t.product_id = s.product_id
    WHEN MATCHED AND s.record_hash != t.record_hash AND s.op='UPDATE' THEN
        UPDATE SET t.product_name = s.product_name, t.product_description = s.product_description, t.product_price = s.product_price, t.product_image = s.product_image, t.product_category_id = s.product_category_id, t.is_deleted = false, t.updated_ts = current_timestamp(), t.op= s.op, t.record_hash = s.record_hash
    
    WHEN MATCHED AND s.is_deleted AND s.op='DELETE' THEN
        UPDATE SET t.is_deleted = true, t.updated_ts = current_timestamp(), t.op= s.op
    
    WHEN NOT MATCHED AND s.op='INSERT' THEN
        INSERT (
            product_id,
            product_name,
            product_description,
            product_price,
            product_image,
            product_category_id,
            is_deleted,
            ingestion_ts,
            ingestion_dt,
            source_system,
            source_file_name,
            batch_id,
            run_id,
            op,
            record_hash,
            created_ts,
            updated_ts
                    )
        VALUES(
            s.product_id,
            s.product_name,
            s.product_description,
            s.product_price,
            s.product_image,
            s.product_category_id,
            s.is_deleted,
            s.ingestion_ts,
            s.ingestion_dt,
            s.source_system,
            s.source_file_name,
            s.batch_id,
            s.run_id,
            s.op,
            s.record_hash,
            current_timestamp(),
            current_timestamp()
            )
    """

# spark.sql(merge_query).show()



# COMMAND ----------

# %run ./products_rescued_fix

# COMMAND ----------

def upsert_to_silver(batch_df, batch_id):

    batch_df = batch_df.cache()
    batch_df.createOrReplaceGlobalTempView(
        "products_cleaned_vw"
    )
    spark.sql(MERGE_UPDATE)

    batch_df.unpersist()

# COMMAND ----------

silver_df = silver_df.drop("event_ts")
# silver_df.printSchema()

# COMMAND ----------


silver_df.writeStream \
    .format("delta") \
    .foreachBatch(upsert_to_silver) \
    .option("checkpointLocation", "dbfs:/Volumes/data/raw/_checkpoints/retails/dev/silver/products_cleaned/") \
    .trigger(once=True) \
    .start() \
    .awaitTermination()

# COMMAND ----------

# quarantine_status	Meaning
# NEW	Newly quarantined record, not yet reviewed
# INVALID	Permanently invalid record
# RESCUED	Record has rescued_data but may be recoverable
# CORRUPT	Completely corrupt JSON/file structure
# PARTIAL_VALID	Some columns valid, some problematic
# FIXED	Data corrected successfully
# REPROCESSED	Re-ingested into clean table

# COMMAND ----------

# GRANTS = "ALTER TABLE retails.silver.products_quarantine OWNER TO `dp-sales-engineers`"
# spark.sql(GRANTS).display()

# COMMAND ----------

