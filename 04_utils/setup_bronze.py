# Databricks notebook source
dbutils.fs.ls("dbfs:/Volumes/data/raw/sales/raw_data/retail/dev/")

# COMMAND ----------

customers_df = spark.read \
    .format("json") \
    .option("inferSchema", "true") \
    .option("mode", "FAILFAST") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load("/Volumes/data/raw/sales/raw_data/retail/dev/customers")

customers_df.printSchema()



# COMMAND ----------

from pyspark.sql.functions import col

# display(customers_df.count())
display(customers_df.dropna().count())
# display(customers_df.dropDuplicates(["phone"]).filter(col("phone").isNull()))
# display(customers_df.filter(col("phone").isNotNull()).count())
# display(customers_df.filter(col("phone").isNull()).count())
# display(customers_df.filter(col("phone").isNotNull()).count())
# display(customers_df)

# COMMAND ----------

inventory_df = spark.read \
    .format("json") \
    .option("mode", "FAILFAST") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load("/Volumes/data/raw/sales/raw_data/retail/dev/inventory")


inventory_df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import to_date, col
display(inventory_df.select(to_date(col("last_updated")).alias("date")).distinct())

# COMMAND ----------

# unique_inventory_df = inventory_df.distinct()
# display(inventory_df.subtract(unique_inventory_df))

dup_df = inventory_df.select("product_id", "store_id").distinct()

display(dup_df.dropDuplicates(["product_id", "store_id"]))


# COMMAND ----------

duplicate_df = inventory_df.groupBy("product_id", "store_id").count().filter(col("count") > 1)
display(duplicate_df)

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/data/raw/sales/raw_data/retail/dev/pos_transactions/"))

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/data/raw/sales/raw_data"))

# COMMAND ----------

spark.read.format("json") \
        .load("/Volumes/data/raw/sales/raw_data/date=2026-03-31/hour=00/").printSchema()


# COMMAND ----------

df = spark.read.format("json") \
        .load("/Volumes/data/raw/sales/raw_data/")

# COMMAND ----------

display(df.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- show schemas in sales;
# MAGIC show grants on catalog sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table retails.bronze.pos_transactions_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- CREATE TABLE retails.bronze.customers_raw
# MAGIC -- USING DELTA
# MAGIC -- LOCATION 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/retails/customers_raw';
# MAGIC -------------------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC -- CREATE TABLE retails.bronze.inventory_raw
# MAGIC -- USING DELTA
# MAGIC -- LOCATION 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/retails/inventory_raw';
# MAGIC -------------------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC -- CREATE TABLE retails.bronze.products_raw
# MAGIC -- USING DELTA
# MAGIC -- LOCATION 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/retails/products_raw';
# MAGIC -------------------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC -- CREATE TABLE retails.bronze.stores_raw
# MAGIC -- USING DELTA
# MAGIC -- LOCATION 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/retails/stores_raw';
# MAGIC -------------------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC -- CREATE TABLE retails.bronze.pos_transactions_raw
# MAGIC -- USING DELTA
# MAGIC -- LOCATION 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/retails/pos_transactions_raw';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ALTER TABLE retails.bronze.pos_transactions_raw OWNER TO `dp-sales-engineers`;
# MAGIC -- ALTER TABLE retails.bronze.customers_raw OWNER TO `dp-sales-engineers`;
# MAGIC -- ALTER TABLE retails.bronze.inventory_raw OWNER TO `dp-sales-engineers`;
# MAGIC -- ALTER TABLE retails.bronze.products_raw OWNER TO `dp-sales-engineers`;
# MAGIC -- ALTER TABLE retails.bronze.stores_raw OWNER TO `dp-sales-engineers`;

# COMMAND ----------

pos_df = spark.read \
    .format("json") \
    .option("mode", "DROPMALFORMED") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load("/Volumes/data/raw/sales/raw_data/retail/dev/pos_transactions/")

# display(pos_df);
pos_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, to_date
display(pos_df.select(to_date(col("date"))).distinct())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ALTER TABLE retails.bronze.pos_transactions_raw
# MAGIC -- ADD COLUMNS (
# MAGIC --   item_id STRING,
# MAGIC --   new_column STRING,
# MAGIC --   order_id STRING,
# MAGIC --   payment_type STRING,
# MAGIC --   price STRING,
# MAGIC --   qty STRING,
# MAGIC --   store_id STRING,
# MAGIC --   txn_time STRING,
# MAGIC --   date DATE,
# MAGIC --   hour INT,
# MAGIC -- -- Metadata columns
# MAGIC --   ingestion_timestamp TIMESTAMP,
# MAGIC --   load_date DATE,
# MAGIC --   source_file_name STRING,
# MAGIC --   record_id STRING,
# MAGIC --   source_system STRING,
# MAGIC --   is_deleted BOOLEAN,
# MAGIC
# MAGIC --   raw_data STRING,
# MAGIC --   _rescued_data STRING
# MAGIC -- );
# MAGIC
# MAGIC   
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC REPLACE TABLE retails.bronze.pos_transactions_raw
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (load_date)
# MAGIC AS SELECT * FROM retails.bronze.pos_transactions_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE retails.bronze.customers_raw
# MAGIC ADD COLUMNS(
# MAGIC   customer_id STRING,
# MAGIC   loyalty_points STRING,
# MAGIC   name STRING,
# MAGIC   phone STRING,
# MAGIC   
# MAGIC -- Metadata columns
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   load_date DATE,
# MAGIC   source_file_name STRING,
# MAGIC   record_id STRING,
# MAGIC   source_system STRING,
# MAGIC   is_deleted BOOLEAN,
# MAGIC
# MAGIC   raw_data STRING,
# MAGIC   _rescued_data STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC REPLACE TABLE retails.bronze.customers_raw
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (load_date)
# MAGIC AS SELECT * FROM retails.bronze.customers_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE retails.bronze.inventory_raw
# MAGIC ADD COLUMNS(
# MAGIC   last_updated STRING,
# MAGIC   product_id STRING,
# MAGIC   stock_qty STRING,
# MAGIC   store_id STRING,
# MAGIC -- Metadata columns
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   load_date DATE,
# MAGIC   source_file_name STRING,
# MAGIC   record_id STRING,
# MAGIC   source_system STRING,
# MAGIC   is_deleted BOOLEAN,
# MAGIC
# MAGIC   raw_data STRING,
# MAGIC   _rescued_data STRING
# MAGIC );
# MAGIC     
# MAGIC REPLACE TABLE retails.bronze.inventory_raw
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (load_date)
# MAGIC AS SELECT * FROM retails.bronze.inventory_raw;

# COMMAND ----------

products_df = spark.read \
    .format("json") \
    .option("mode", "DROPMALFORMED") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load("/Volumes/data/raw/sales/raw_data/retail/dev/products/")

# display(products_df);
products_df.printSchema()


# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE retails.bronze.products_raw
# MAGIC ADD COLUMNS(
# MAGIC   category STRING,
# MAGIC   price STRING,
# MAGIC   product_id STRING,
# MAGIC   product_name STRING,
# MAGIC -- Metadata columns
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   load_date DATE,
# MAGIC   source_file_name STRING,
# MAGIC   record_id STRING,
# MAGIC   source_system STRING,
# MAGIC   is_deleted BOOLEAN,
# MAGIC
# MAGIC   raw_data STRING,
# MAGIC   _rescued_data STRING
# MAGIC );
# MAGIC     
# MAGIC REPLACE TABLE retails.bronze.products_raw
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (load_date)
# MAGIC AS SELECT * FROM retails.bronze.products_raw;

# COMMAND ----------

stores_df = spark.read \
    .format("json") \
    .option("mode", "DROPMALFORMED") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load("/Volumes/data/raw/sales/raw_data/retail/dev/stores/")

# display(stores_df);
stores_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE retails.bronze.stores_raw
# MAGIC ADD COLUMNS(
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   store_id STRING,
# MAGIC -- Metadata columns
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   load_date DATE,
# MAGIC   source_file_name STRING,
# MAGIC   record_id STRING,
# MAGIC   source_system STRING,
# MAGIC   is_deleted BOOLEAN,
# MAGIC
# MAGIC   raw_data STRING,
# MAGIC   _rescued_data STRING
# MAGIC );
# MAGIC     
# MAGIC REPLACE TABLE retails.bronze.stores_raw
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (load_date)
# MAGIC AS SELECT * FROM retails.bronze.stores_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended retails.bronze.pos_transactions_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC show grants on catalog retails;

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas in retails;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe schema extended retails.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in retails.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC show grants on table retails.bronze.pos_transactions_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC show grants on schema retails.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC show grants on schema retails.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs;

# COMMAND ----------

dbutils.fs.ls("dbfs:/Volumes/data/raw/_schemas/retails/dev/customers/_schemas/")
# dbutils.fs.ls("dbfs:/Volumes/data/raw/_schemas/retails/dev/");



# COMMAND ----------

dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/customers")

# COMMAND ----------

dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/customers/", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in retails.bronze;
# MAGIC -- drop table retails.bronze.customers12;

# COMMAND ----------

# creating checkpoint locations for auto loader for retails
dbutils.fs.mkdirs("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/customers")
dbutils.fs.mkdirs("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/inventory")
dbutils.fs.mkdirs("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/pos_transactions")
dbutils.fs.mkdirs("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/products")
dbutils.fs.mkdirs("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/stores")

# creating schema locations  for auto loader for retails
dbutils.fs.mkdirs("dbfs:/Volumes/data/raw/_schemas/retails/dev/customers")
dbutils.fs.mkdirs("dbfs:/Volumes/data/raw/_schemas/retails/dev/inventory")
dbutils.fs.mkdirs("dbfs:/Volumes/data/raw/_schemas/retails/dev/pos_transactions")
dbutils.fs.mkdirs("dbfs:/Volumes/data/raw/_schemas/retails/dev/products")
dbutils.fs.mkdirs("dbfs:/Volumes/data/raw/_schemas/retails/dev/stores")

# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/test", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC show volumes in data.raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history retails.bronze.inventory_raw;

# COMMAND ----------

# dbutils.fs.rm("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/bronze/customers/", True)
# dbutils.fs.rm("dbfs:/Volumes/data/raw/_schemas/retails/dev/customers/", True)

# dbutils.fs.ls("dbfs:/Volumes/data/raw/_checkpoints/retails/dev/stores")
# dbutils.fs.ls("dbfs:/Volumes/data/raw/_schemas/retails/dev/stores")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE retails.bronze.inventory_raw;
# MAGIC -- DROP TABLE retails.bronze.products_raw;
# MAGIC -- DROP TABLE retails.bronze.customers_raw;
# MAGIC -- DROP TABLE retails.bronze.stores_raw;
# MAGIC -- DROP TABLE retails.bronze.pos_transactions_raw;

# COMMAND ----------

pos_df = spark.read.format("json") \
      .load("dbfs:/Volumes/data/raw/sales/raw_data/retail/dev/pos_transactions/")

pos_df.display()

# COMMAND ----------

from pyspark.sql.functions import col
# pos_df.select(pos_df.date.alias("transaction_date")).distinct().limit(10).display()
pos_df.withColumnRenamed("date", "txn_date").select("txn_date").distinct().limit(10).display()

# COMMAND ----------

from pyspark.sql.functions import col
# display(pos_df.select("_corrupt_record", "order_id").filter(col("_corrupt_record").isNotNull()).count())
# pos_df.select("_corrupt_record", "order_id").filter(col("_corrupt_record").isNull()).count()

# pos_df.select("_corrupt_record", "order_id") \
#       .filter(col("_corrupt_record").isNotNull()) \
#       .count()

# pos_df_cached = pos_df.cache()
# pos_df_cached.filter(col("_corrupt_record").isNotNull()).count()

# pos_df = pos_df.persist()
# display(pos_df.filter(col("_corrupt_record").isNotNull()).count())

pos_df.createOrReplaceTempView("pos")

spark.sql("""
SELECT COUNT(*) 
FROM pos 
WHERE _corrupt_record IS NOT NULL
""").show()

# COMMAND ----------

# remove delta logs
dbutils.fs.rm("s3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/retails/pos_transactions_raw/", True)

# COMMAND ----------

customers_df = spark.read.table("retails.bronze.customers_raw")
# print(customers_df.columns)
customers_df.printSchema()


# COMMAND ----------

print(spark.version)

# COMMAND ----------

from pyspark.sql.functions import col

spark.table("retails.bronze.inventory_raw").filter(col("ingestion_dt") > "2026-05-02").display()

# COMMAND ----------

spark.read \
    .format("csv") \
    .schema("product_id string, store_id string") \
    .load("dbfs:/Volumes/data/raw/inventory/raw_data/retail/dev/inventory/")