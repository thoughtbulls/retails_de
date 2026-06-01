# Databricks notebook source
# Method COPY-INTO, prepare the source location with data "/Volumes/data/raw/sales/read_input/orders/"
date = "2013-07-28"
dbutils.fs.cp(f"/Volumes/data/raw/sales/partitioned_data/retail_db/orders/order_date={date}/", f"/Volumes/data/raw/sales/read_input/orders/order_date={date}/", recurse=True)



# COMMAND ----------

# verify the data is in the source location
dbutils.fs.ls("/Volumes/data/raw/sales/read_input/orders")

# COMMAND ----------

#  Now ingest data using COPY-INTO
orders_df = spark.sql("""
COPY INTO sales.bronze.orders_raw
FROM '/Volumes/data/raw/sales/read_input/orders'

FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  'inferSchema' = 'false'
)
COPY_OPTIONS (
    'mergeSchema' = 'true'
)
""")



# COMMAND ----------

from pyspark.sql.functions import cast, col
# verify the data is in the target table
display(spark.read.table('sales.bronze.orders_raw').orderBy(col("order_date").desc(), col("order_id").cast("int").asc()))

# COMMAND ----------

order_df = spark.read.option("header", "true").csv('/Volumes/data/raw/sales/read_input/orders')
order_df.printSchema()


# COMMAND ----------

orders_tbl_df = spark.sql("""
                          select * from sales.bronze.orders_raw
                          """)
orders_tbl_df.count()

# COMMAND ----------

spark.read.option("header", "true") \
    .csv('/Volumes/data/raw/sales/partitioned_data/retail_db/orders').orderBy("order_id").printSchema()

# COMMAND ----------

