# Databricks notebook source
# MAGIC %md
# MAGIC Generally we use ingestion system like:
# MAGIC 1. (fixed schema + cloudFiles.schemaEvolutionMode = rescue) + (_rescued_data + _corrupt_record) == ERP, SAS
# MAGIC
# MAGIC - Why?
# MAGIC     Because:
# MAGIC     - source contracts usually exist
# MAGIC     - structure mostly stable
# MAGIC     - but unexpected fields still happen
# MAGIC     - pipelines should remain resilient
# MAGIC
# MAGIC - Typical systems
# MAGIC         ✔ ERP
# MAGIC         ✔ Salesforce
# MAGIC         ✔ SAP
# MAGIC         ✔ SaaS APIs
# MAGIC         ✔ CDC feeds
# MAGIC 2. (strict schema + cloudFiles.schemaEvolutionMode = failOnNewColumns) + (_rescued_data + _corrupt_record) == Banking, follow the compliance.
# MAGIC
# MAGIC - Why?
# MAGIC     Because:
# MAGIC     - unexpected data may indicate risk
# MAGIC     - audit/compliance requirements
# MAGIC     - downstream calculations sensitive
# MAGIC
# MAGIC - Typical systems
# MAGIC         ✔ Banking core system
# MAGIC         ✔ Payment System
# MAGIC         ✔ Trading Capital market
# MAGIC         ✔ Insurance systems
# MAGIC         ✔ Healthcare systems
# MAGIC         ✔ Govermant/Tax tax
# MAGIC
# MAGIC 3. (no schema + cloudFiles.schemaEvolutionMode = addNewColumns + schemaHints) + (_rescued_data + _corrupt_record) == logs , event streams, telemetry, etc
# MAGIC
# MAGIC - Why?
# MAGIC     Because:
# MAGIC     - no predefined schema
# MAGIC     - dynamic JSON payloads
# MAGIC     - frequent evolution
# MAGIC
# MAGIC - Typical systems
# MAGIC         ✔ IoT
# MAGIC         ✔ clickstream
# MAGIC         ✔ application logs
# MAGIC         ✔ Kafka events
# MAGIC         ✔ telemetry

# COMMAND ----------

from pyspark.sql.functions import col

schema = """
product_id STRING,
product_category_id STRING,
product_name STRING,
product_description STRING,
product_price DOUBLE,
product_image STRING,
op STRING,
_corrupt_record STRING
"""

df = (spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") 
    .option("cloudFiles.schemaLocation", "dbfs:/Volumes/data/raw/_schemas/retails/dev/products/") \
    .schema(schema) \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .option("cloudFiles.rescuedDataColumn", "_rescued_data") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load("dbfs:/Volumes/data/raw/dev/retail_db/products/") \
)


# COMMAND ----------

# from pyspark.sql.functions import col

# df = spark.read \
#             .format("json") \
#             .schema(schema) \
#             .load("dbfs:/Volumes/data/raw/dev/retail_db/products/") \
#             .filter(col("product_category_id") == 2)

# COMMAND ----------

from pyspark.sql.functions import to_json, struct, col, current_timestamp, current_date, regexp_extract, expr, lit, StringType, coalesce

df = df.withColumn("raw_data", to_json(struct("*"))) \
        .withColumn(
            "source_file_name",
            regexp_extract(col("_metadata.file_path"), r'([^/]+$)', 1)
        ) \
        .withColumn("source_file_path", col("_metadata.file_path")) \
        .withColumn("replay_flag", lit(False)) \
        .withColumn("op", coalesce(col("op"), lit("INSERT"))
        
    )


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

def process_batch(df, batch_id):

    df = (
        df
        .withColumn("product_price", col("product_price").cast(DoubleType())) \
        .withColumn("ingestion_ts", current_timestamp()) \
        .withColumn("ingestion_dt", current_date()) \
        .withColumn("batch_id", lit(batch_id)) \
        .withColumn("run_id", lit(run_id)) \
        .withColumn("source_system", lit("ERP"))
    )

    (
        df.write
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("retails.bronze.products_raw")
    )

# COMMAND ----------


(df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "dbfs:/Volumes/data/raw/_checkpoints/retails/dev/bronze/products/") \
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# GRANTS = "ALTER TABLE retails.bronze.products_raw OWNER TO `dp-sales-engineers`"
# spark.sql(GRANTS).display()

# COMMAND ----------

# dbutils.fs.ls("dbfs:/Volumes/data/raw/_schemas/retails/dev/products/")
# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/bronze/products/")
# dbutils.fs.rm("dbfs:/Volumes/data/raw/_schemas/retails/dev/products/", True)
# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/bronze/products/", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- describe table extended retails.bronze.products_raw;
# MAGIC -- describe detail retails.bronze.products_raw;
# MAGIC -- truncate table retails.bronze.products_raw;
# MAGIC -- truncate table retails.silver.products_quarantine;
# MAGIC
# MAGIC select * from retails.bronze.products_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select product_id, `_rescued_data`, `_corrupt_record` from retails.bronze.products_raw where product_category_id=3
# MAGIC -- select product_id, op, city, `_rescued_data`, _corrupt_record, run_id, batch_id from retails.bronze.products_raw where product_category_id=8 and run_id='20260514_131526'
# MAGIC -- order by ingestion_ts desc;
# MAGIC
# MAGIC -- select product_category_id from retails.bronze.products_raw where product_id=144;
# MAGIC -- select * from retails.bronze.products_raw where source_file_name like 'late%'
# MAGIC
# MAGIC -- select product_category_id, count(*) from retails.bronze.products_raw 
# MAGIC --     group by product_category_id order by count(*) desc;
# MAGIC -- describe table retails.bronze.products_raw;
# MAGIC
# MAGIC -- truncate table retails.bronze.products_raw;

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE retails.bronze.products_raw(
#     product_id string,
#     product_name string,
#     product_description string,
#     product_price double,
#     product_image string,
#     _corrupt_record string,
#     _rescued_data string,
#     raw_data string,
#     ingestion_ts timestamp,
#     ingestion_dt date,
#     batch_id int,
#     run_id string,
#     source_file_name string,
#     source_system string,
#     op string,
#     replay_flag boolean,
#     source_file_path string

# )
# USING DELTA
# PARTITIONED BY (ingestion_dt)
# LOCATION 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/retails/products_raw'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from retails.bronze.products_raw where product_category_id=4;
# MAGIC -- truncate table retails.bronze.products_raw;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- describe volume data.raw.dev;

# COMMAND ----------

