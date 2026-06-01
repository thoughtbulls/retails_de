# Databricks notebook source
spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "json")\
    .option("cloudFiles.schemaLocation", "/FileStore/tables/schema")\
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
    .load("/FileStore/tables/")\
    .writeStream.format("delta").option("checkpointLocation", "/FileStore/tables/checkpoint")\
    .start("/FileStore/tables/delta")

# COMMAND ----------

d = "/Volumes/data/raw/sales/tmp_stream_data"
checkpoints = "/Volumes/data/raw/sales/checkpoints/orders_stream"

spark.createDataFrame([("Hello",), ("World",)]) \
    .write.mode("overwrite").format("text").save(d)

q = spark.readStream.format("text") \
    .load(d) \
    .writeStream.format("console") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", checkpoints) \
    .start()

q.awaitTermination()

# COMMAND ----------

dbutils.fs.ls("/Volumes/data/raw/sales/partitioned_data/retail_db/orders/")
# dbutils.fs.ls("/Volumes/data/raw/sales/checkpoints/orders_stream")

# COMMAND ----------

dbutils.fs.rm("dbfs:/Volumes/data/raw/sales/checkpoints", recurse=True)

# COMMAND ----------

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/Volumes/data/raw/sales/partitioned_data/retail_db/orders/")

# COMMAND ----------

# df.limit(10).display()
df.printSchema()

# COMMAND ----------

df.select("order_date").distinct().display()

# COMMAND ----------

dbutils.fs.ls("dbfs:/Volumes/data/raw/sales")

# COMMAND ----------

dbutils.fs.ls("dbfs:/Volumes/data/raw/sales/retail_db/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/Volumes/data/raw/sales/retail_db/orders")

# COMMAND ----------

original_data_df = spark.read \
    .format("csv") \
    .load("dbfs:/Volumes/data/raw/sales/retail_db/orders")

# COMMAND ----------

# original_data_df.limit(5).display()
from pyspark.sql.functions import col
from pyspark.sql.types import DateType

# original_data_df.select(col("_c1").cast(DateType())).distinct().display()
original_data_df.filter(col("_c1").cast(DateType()) == "2013-08-03").display()


# COMMAND ----------

original_data_df.printSchema()

# COMMAND ----------

dbutils.fs.ls("dbfs:/Volumes/data/raw/sales")

# COMMAND ----------

dbutils.fs.ls("dbfs:/Volumes/data/raw/sales/read_input/orders/")

# COMMAND ----------

read_input_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/Volumes/data/raw/sales/read_input/orders/")
read_input_df.orderBy("order_id").limit(5).display()

# COMMAND ----------

read_input_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
read_input_df.select(col("order_id").cast("integer")).orderBy("order_id").limit(5).display()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
read_input_df.select(col("order_id").cast(IntegerType())).orderBy("order_id").limit(5).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in sales.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales.bronze.customers_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended sales.bronze.customers_raw;

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

_sqldf.select("customer_fname", "customer_lname").display()

# COMMAND ----------

customers_raw_df = spark.read.table("sales.bronze.customers_raw")

# COMMAND ----------

customers_raw_df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE EXTERNAL VOLUME sales.bronze.vol_sales
# MAGIC -- LOCATION 's3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/sales/vol_sales';

# COMMAND ----------

# MAGIC %sql
# MAGIC show volumes in sales.bronze;
# MAGIC -- describe volume data.raw.sales;
# MAGIC -- drop volume sales.bronze.vol_sales;

# COMMAND ----------

# dbutils.fs.mkdirs("s3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/sales/vol_sales")
# dbutils.fs.rm("s3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/sales/volumes")

# COMMAND ----------

# dbutils.fs.ls("s3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/sales/")
dbutils.fs.ls("dbfs:/Volumes/sales/bronze/vol_sales/checkpoints")
dbutils.fs.ls("dbfs:/Volumes/sales/bronze/vol_sales/schemas")

# COMMAND ----------

# create check points location and chema location
# dbutils.fs.mkdirs("s3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/sales/vol_sales/checkpoints/customers")
# dbutils.fs.mkdirs("dbfs:/Volumes/sales/bronze/vol_sales/checkpoints/orders")

# dbutils.fs.mkdirs("s3://thoughtbulls-dp-uc-root-ap-south-1-8affd0fb/external_data/bronze/sales/vol_sales/schemas/customers")
# dbutils.fs.mkdirs("dbfs:/Volumes/sales/bronze/vol_sales/schemas/orders")

# COMMAND ----------

dbutils.notebook.exit("Restarting")

# COMMAND ----------

checkpoints = "dbfs:/Volumes/sales/bronze/vol_sales/checkpoints/orders"
schemas = "dbfs:/Volumes/sales/bronze/vol_sales/schemas/orders"
raw_data_orders = "dbfs:/Volumes/data/raw/sales/read_input/orders"
# auto loader read data from volume dbfs:/Volumes/data/raw/orders
df_orders_2 = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", schemas)
      .load(raw_data_orders))


# COMMAND ----------

df_orders_2.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", checkpoints) \
    .toTable("sales.bronze.orders_raw")
            
            

# COMMAND ----------

spark.read.table("sales.bronze.orders_raw").printSchema()

# COMMAND ----------

df_orders_2.printSchema()

# COMMAND ----------

display(df_orders_2)

# COMMAND ----------

dbutils.fs.ls("dbfs:/Volumes/data/raw/sales/read_input/orders")

# COMMAND ----------

dbutils.fs.ls("dbfs:/Volumes/data/raw/sales/partitioned_data/retail_db/orders")

# COMMAND ----------

dbutils.fs.cp("/Volumes/data/raw/sales/partitioned_data/retail_db/orders/order_date=2013-08-08","/Volumes/data/raw/sales/read_input/orders/order_date=2013-08-08", recurse=True) 

# COMMAND ----------

from pyspark.sql.functions import try_to_date
help(try_to_date)

# COMMAND ----------

