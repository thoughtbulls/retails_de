# Databricks notebook source
# MAGIC %md
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

schema = """
        customer_id STRING,
        customer_fname STRING,
        customer_lname STRING,
        customer_email STRING,
        customer_password STRING,
        customer_street STRING,
        customer_city STRING,
        customer_state STRING,
        customer_zipcode STRING
        """

# Read customers using auto loader
df = (spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") 
    .option("cloudFiles.schemaLocation", "dbfs:/Volumes/data/raw/_schemas/retails/dev/customers/") \
    .option("cloudFiles.maxFilesPerTrigger", "1") \
    .option("schema", schema) \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .option("cloudFiles.rescuedDataColumn", "_rescued_data") \
    .option("cloudFiles.schemaHints", "customer_id INTEGER, customer_zipcode INTEGER, _corrupt_record STRING") \
    .load("dbfs:/Volumes/data/raw/dev/retail_db/customers/")
)


# COMMAND ----------

from pyspark.sql.functions import to_json, struct, col, current_timestamp, current_date, regexp_extract, expr, lit, StringType

df = df.withColumn("raw_data", to_json(struct("*"))) \
        .withColumn(
            "source_file_name",
            regexp_extract(col("_metadata.file_path"), r'([^/]+$)', 1)
        ) \
        .withColumn("source_file_path", col("_metadata.file_path")) \
        .withColumn("replay_flag", lit(False))


if "op" in df.columns:
    df = df.withColumn("op", col("op").cast(StringType()))
else:
    df = df.withColumn("op", lit("INSERT").cast(StringType()))


# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

def process_batch(df, batch_id):

    df = (
        df
        .withColumn("customer_id", col("customer_id").cast("String")) \
        .withColumn("customer_zipcode", col("customer_zipcode").cast("String")) \
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
            .saveAsTable("retails.bronze.customers_raw")
    )

# COMMAND ----------


(df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "dbfs:/Volumes/data/raw/_checkpoints/retails/dev/bronze/customers/") \
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# GRANTS = "ALTER TABLE retails.bronze.customers_raw OWNER TO `dp-sales-engineers`"
# spark.sql(GRANTS).display()

# COMMAND ----------

# dbutils.fs.ls("dbfs:/Volumes/data/raw/_schemas/retails/dev/customers/")
# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/bronze/customers/")
# dbutils.fs.rm("dbfs:/Volumes/data/raw/_schemas/retails/dev/customers/", True)
# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/bronze/customers/", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- describe table extended retails.bronze.customers_raw;
# MAGIC select * from retails.bronze.customers_raw;
# MAGIC
# MAGIC -- drop table if exists retails.bronze.customers_raw;

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE retails.bronze.customers_raw (
#   customer_id STRING,
#   customer_fname STRING,
#   customer_lname STRING,
#   customer_email STRING,
#   customer_password STRING,
#   customer_street STRING,
#   customer_city STRING,
#   customer_state STRING,
#   customer_zipcode STRING,
#   _corrupt_record string,
#   _rescued_data string,
#   raw_data string,
#   ingestion_ts timestamp,
#   ingestion_dt date,
#   batch_id int,
#   run_id string,
#   source_file_name string,
#   source_system string
# )
# USING DELTA
# PARTITIONED BY (ingestion_dt)
# LOCATION 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/retails/customers_raw'

# COMMAND ----------

