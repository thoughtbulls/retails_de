# Databricks notebook source
# -- # Read clean products from silver
silver_df = (
    spark.readStream \
        .format("delta") \
        .table("retails.silver.products_cdc")
)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, sha2, concat_ws

# Prepare dataframe for gold SCD2
silver_df = (
    silver_df
        .withColumn("product_price", col("product_price").cast("double")) \

        .select(
            "product_id",
            "product_name",
            "product_description",
            "product_price",
            "product_image",
            "product_category_id",
            "op"
        ) \

        .withColumn("is_active", lit(True).cast("boolean")) \
        .withColumn("effective_from", current_timestamp()) \
        .withColumn(
            "effective_to",
            lit(None).cast("timestamp")
        ) \
        .withColumn("is_current", lit(True).cast("boolean")) \
        .withColumn("created_ts", current_timestamp()) \
        .withColumn("updated_ts", current_timestamp()) \
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
                    ))
    


# COMMAND ----------

# STEP-1: Expire old current rows
MERGE_EXPIRE = """
MERGE INTO retails.gold.dim_products t
USING global_temp.silver_products_vw s

ON t.product_id = s.product_id
AND t.is_current = true

WHEN MATCHED AND s.op = 'UPDATE'
THEN UPDATE SET
    t.is_active = false,
    t.is_current = false,
    t.effective_to = current_timestamp(),
    t.updated_ts = current_timestamp()

WHEN MATCHED AND s.op = 'DELETE'
THEN UPDATE SET 
    t.is_active = false,
    t.is_current = false,
    t.effective_to = current_timestamp(),
    t.updated_ts = current_timestamp()
"""

# COMMAND ----------

# STEP-2: Insert new versions
INSERT_NEW = """
INSERT INTO retails.gold.dim_products(
    product_id,
    product_name,
    product_description,
    product_price,
    product_image,
    product_category_id,
    is_active,
    effective_from,
    effective_to,
    is_current,
    created_ts,
    updated_ts)

SELECT
    s.product_id,
    s.product_name,
    s.product_description,
    s.product_price,
    s.product_image,
    s.product_category_id,
    true,
    current_timestamp(),
    CAST(NULL AS TIMESTAMP),
    true,
    current_timestamp(),
    current_timestamp()

FROM global_temp.silver_products_vw s

WHERE s.op IN ('INSERT', 'UPDATE')
"""

# COMMAND ----------

# foreachBatch function
def upsert_to_gold(batch_df, batch_id):

    batch_df.createOrReplaceGlobalTempView(
        "silver_products_vw"
    )

    spark.sql(MERGE_EXPIRE).show()

    spark.sql(INSERT_NEW).show()

    batch_df.unpersist()

# COMMAND ----------

# Start streaming query
query = (
    silver_df.writeStream
        .foreachBatch(upsert_to_gold)
        .option(
            "checkpointLocation",
            "dbfs:/Volumes/data/raw/_checkpoints/retails/dev/gold/products_cdc/"
        )
        .trigger(availableNow=True)
        .start()
)

query.awaitTermination()

# COMMAND ----------

# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/gold/products/")
# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/gold/products/", True)

# COMMAND ----------

