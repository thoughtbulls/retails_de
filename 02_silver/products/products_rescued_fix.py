# Databricks notebook source
from pyspark.sql.functions import col

# =========================================================
# STEP-5 : GET RAW INVALID RECORDS FROM QUARANTINE
# =========================================================

invalid_df = spark.read.table("retails.silver.products_quarantine") \
                    .filter((col("_rescued_data").isNotNull()) & (col("quarantine_status") == 'NEW'))

# COMMAND ----------

from pyspark.sql.functions import col, get_json_object as get_json_from

# =========================================================
# STEP-6 : TRY TO RECOVER RESCUED COLUMNS
# =========================================================
recovered_df = invalid_df \
                .withColumn("product_id_fixed", get_json_from(col("_rescued_data"), "$.product_id")) \
                .withColumn("product_name_fixed", get_json_from(col("_rescued_data"), "$.product_name")) \
                .withColumn("product_price_fixed", get_json_from(col("_rescued_data"), "$.product_price")) \
                .withColumn("product_category_id_fixed", get_json_from(col("_rescued_data"), "$.product_category_id"))


# COMMAND ----------

from pyspark.sql.functions import coalesce, trim

# =========================================================
# STEP-7 : MERGE RECOVERED VALUES
# =========================================================

recovered_df = recovered_df \
    .withColumn(
        "product_id",
        coalesce(col("product_id"), col("product_id_fixed").cast("bigint"))
    ) \
    .withColumn(
        "product_name",
        coalesce(col("product_name"), trim(col("product_name_fixed")))
    ) \
    .withColumn(
        "product_price",
        coalesce(col("product_price"), col("product_price_fixed").cast("double"))
    ) \
    .withColumn(
        "category",
        coalesce(col("product_category_id"), col("product_category_id_fixed").cast("bigint"))
    )

# COMMAND ----------

recovered_df = recovered_df.drop("product_id_fixed", "product_name_fixed", "product_price_fixed", "product_category_id_fixed")

# COMMAND ----------

from pyspark.sql.functions import col

# =========================================================
# STEP-8 : APPLY DATA QUALITY RULES
# =========================================================

cleaned_recovered_df = recovered_df.filter(
    col("product_id").isNotNull() &
    col("product_name").isNotNull() &
    col("product_price").isNotNull() &
    (col("product_price") >= 0)
)

# COMMAND ----------

cleaned_recovered_df = cleaned_recovered_df.dropDuplicates(["product_id"])
cleaned_recovered_df.createOrReplaceTempView("products_cleaned_vw_fixed")

# COMMAND ----------

from pyspark.sql.functions import when, current_timestamp, sha2, concat_ws

cleaned_recovered_df = cleaned_recovered_df \
    .withColumn("product_id", col("product_id").cast("bigint")) \
    .withColumn("product_price", col("product_price").cast("decimal(10,2)")) \
    .withColumn("product_category_id", col("product_category_id").cast("bigint")) \
    .withColumn("batch_id", col("batch_id").cast("integer")) \
    .withColumn("is_deleted", when(col("op")=='DELETE', True).otherwise(False)) \
    .withColumn("product_description", when(col("product_description").isNull(), "Unknown").otherwise(col("product_description")))

cleaned_recovered_df = cleaned_recovered_df.select("product_id", "product_name", "product_description", "product_price", "product_image", "product_category_id", "op", "is_deleted", "source_system", "source_file_name", "ingestion_ts", "ingestion_dt", "batch_id", "run_id")

cleaned_recovered_df = cleaned_recovered_df.withColumn("event_ts", current_timestamp()) \
                    .withColumn("record_hash",
                                sha2(
                                    concat_ws(
                                        "||",
                                        col("product_id"),
                                        col("product_name"),
                                        col("product_description"),
                                        col("product_price"),
                                        col("product_category_id")
                                    ),
                                    256
                                )
                            )
    

try:
    cleaned_recovered_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "false") \
        .saveAsTable("retails.silver.products_cdc")

except Exception as e:
    print(str(e))

# COMMAND ----------

merge_query_rescued = """
    MERGE INTO retails.silver.products_quarantine t
    USING products_cleaned_vw_fixed s
    ON t.product_id = s.product_id
    WHEN MATCHED THEN
        UPDATE SET t.quarantine_status = 'FIXED', t.reprocessed_at = current_timestamp()
 
    """
spark.sql(merge_query_rescued).show()


# COMMAND ----------

update_query_corrupt = """
        UPDATE retails.silver.products_quarantine
        SET
            quarantine_status = 'INVALID',
            reprocessed_at = current_timestamp()
        WHERE quarantine_status = 'NEW'
"""

spark.sql(update_query_corrupt).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from retails.silver.products_quarantine;
# MAGIC -- select * from retails.silver.products_cdc;
# MAGIC

# COMMAND ----------

