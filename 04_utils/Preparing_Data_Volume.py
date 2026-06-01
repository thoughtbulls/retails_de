# Databricks notebook source
dbutils.fs.ls("/Volumes/data/raw/sales/retail_db/orders")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, DecimalType
schema = StructType([
  StructField("order_item_id", IntegerType(), True),
  StructField("order_item_order_id", IntegerType(), True),
  StructField("order_item_product_id", IntegerType(), True),
  StructField("order_item_quantity", IntegerType(), True),
  StructField("order_item_subtotal", DecimalType(10, 2), True),
  StructField("order_item_product_price", DecimalType(10, 2), True)
])

# COMMAND ----------

order_items_df = spark.read.csv("/Volumes/data/raw/sales/retail_db/order_items/", schema=schema)
display(order_items_df)

# COMMAND ----------

order_items_df.select("order_item_order_id").distinct().count()

# COMMAND ----------

from pyspark.sql.types import TimestampType, StringType, StructType, StructField, IntegerType

orders_schema = StructType([
  StructField("order_id", IntegerType(), True),
  StructField("order_date", TimestampType(), True),
  StructField("order_customer_id", IntegerType(), True),
  StructField("order_status", StringType(), True)
])

order_df = spark.read.csv("/Volumes/data/raw/sales/retail_db/orders/", schema=orders_schema)
display(order_df)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import DateType

order_df.select(col("order_date").cast(DateType())).distinct().count()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import DateType

orders_df = order_df.select("order_id", col("order_date").cast(DateType()), "order_customer_id", "order_status" )
orders_df.display()

# COMMAND ----------

# writing orders to partitioned data by order_date
orders_df.write.mode("overwrite")\
    .option("path", "/Volumes/data/raw/sales/partitioned_data/retail_db/orders")\
    .partitionBy("order_date")\
    .format("csv")\
    .option("header", "true")\
    .save()
    

# COMMAND ----------

# writing orders to partitioned data by order_customer_id

orders_df.write.mode("overwrite")\
    .option("path", "/Volumes/data/raw/sales/partitioned_data/retail_db/orders_by_customer/orders")\
    .partitionBy("order_customer_id")\
    .format("csv")\
    .option("header", "true")\
    .save()
    

# COMMAND ----------

orders_df.select("order_customer_id").distinct().count()

# COMMAND ----------

spark.read.table("sales.bronze.orders_raw").display()

# COMMAND ----------

orders_df.select("order_customer_id").distinct()

# COMMAND ----------

dbutils.fs.rm("/Volumes/data/raw/sales/read_input/orders/", recurse=True)

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/data/raw/sales/partitioned_data/retail_db")

# COMMAND ----------

keep_to_file = "order_date=2013-07-25"
for file in dbutils.fs.ls("/Volumes/data/raw/sales/read_input/orders/"):
    if keep_to_file not in file.name:
         dbutils.fs.rm(file.path, recurse=True)


# COMMAND ----------

df = spark.read.option("header", "true").csv("/Volumes/data/raw/sales/partitioned_data/retail_db/orders_by_customer/orders/order_customer_id=1")

# COMMAND ----------

spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/data/raw/sales/partitioned_data/retail_db/orders_by_customer/orders/order_customer_id=10/").printSchema()

# COMMAND ----------

