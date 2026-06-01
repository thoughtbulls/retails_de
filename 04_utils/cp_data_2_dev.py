# Databricks notebook source
# dbutils.fs.cp("/Volumes/data/raw/partitioned_data/retail_db/products/product_category_id=5/", "/Volumes/data/raw/dev/retail_db/products/product_category_id=5/", recurse=True)

dbutils.fs.cp("/Volumes/data/raw/partitioned_data/retail_db/json_data/products/product_category_id=4/", "/Volumes/data/raw/dev/retail_db/products/product_category_id=4/", recurse=True)

# COMMAND ----------

dbutils.fs.ls("/Volumes/data/raw/dev/retail_db/products/")
# dbutils.fs.ls("/Volumes/data/raw/partitioned_data/retail_db/json_data/products/product_category_id=11/")

# dbutils.fs.ls("/Volumes/data/raw/partitioned_data/retail_db/json_data_medium_size/order_items/order_item_product_id=35/")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe volume data.raw.dev

# COMMAND ----------

# dbutils.fs.rm("/Volumes/data/raw/dev/retail_db/products/late_arriving_data.json", recurse=True)
# dbutils.fs.mkdirs("/Volumes/data/raw/partitioned_data/retail_db/json_data")
# dbutils.fs.ls("/Volumes/data/raw/partitioned_data/retail_db/json_data_medium_size/order_items/")


# COMMAND ----------

# MAGIC %sql
# MAGIC describe volume data.raw.partitioned_data;

# COMMAND ----------

# dbutils.fs.rm("/Volumes/data/raw/partitioned_data/retail_db/json_data/order_items/", recurse=True)
# dbutils.fs.rm("/Volumes/data/raw/partitioned_data/retail_db/json_data_file/orders/", recurse=True)

# COMMAND ----------

import os
path = "/Volumes/data/raw/dev/retail_db/categories/"
bytes_size = os.path.getsize(path)
mb_size = bytes_size / (1024 * 1024)
print(f"{mb_size:.2f} MB")
print(bytes_size)

# COMMAND ----------

dbutils.fs.cp("/Volumes/data/raw/retail_db/order_items", "/Workspace/Users/db.neha_verma.eng@zohomail.in/data_files/order_items", recurse=True)

# COMMAND ----------

schema = """
        order_item_id STRING,
        order_item_order_id STRING,
        order_item_product_id STRING,
        order_item_quantity STRING,
        order_item_subtotal STRING,
        order_item_product_price STRING
        """

df_cat=spark.read \
    .format("json") \
    .schema(schema) \
    .load("/Volumes/data/raw/dev/retail_db/order_items/")

# COMMAND ----------

display(df_cat)


# COMMAND ----------

sc = spark.sparkContext

# COMMAND ----------

# sc.master
print(sc.parallelize([1,2,3,4], 3).collect())
print(sc.parallelize([1,2,3,4], 3).glom().collect())
rdd = sc.parallelize([1,2,3,4], 3)
# print(rdd.getNumPartitions())
print(rdd.collect())

# COMMAND ----------

# sc.defaultParallelism
help(sc.parallelize)

# COMMAND ----------

spark.sparkContext.getConf().getAll()

# COMMAND ----------

sc.getConf().getAll()

# COMMAND ----------

# sc.getConf().get('spark.databricks.io.cache.initialDiskSize')
print(sc.getConf().get('spark.executor.memory'))
print(sc.getConf().get('spark.driver.memory'))
print(sc.getConf().get('spark.sql.files.maxPartitionByte'))
print(sc.getConf().get('spark.databricks.io.cache.maxDiskUsage'))

# COMMAND ----------

spark.conf.get('spark.sql.shuffle.partitions')

# COMMAND ----------

# spark.conf.set("spark.executor.instances", "4")
# spark.conf.set("spark.executor.cores", "4")

# COMMAND ----------

try:    spark.conf.get("spark.executor.instances")
except Exception:
    print("Configuration 'spark.executor.instances' not found.")


# COMMAND ----------

for k,v in sc.getConf().getAll():
    if "executor.memory" in k:
        print(k,v)

# COMMAND ----------

dbutils.fs.ls("/Volumes/data/raw/partitioned_data/retail_db/json_data_small_size_m/order_items")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe volume data.raw.partitioned_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe volume data.raw.dev;

# COMMAND ----------

# MAGIC %sql
# MAGIC