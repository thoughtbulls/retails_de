# Databricks notebook source
base_path = "/Volumes/data/raw/retail_db/"
file = "order_items"

# COMMAND ----------

schema_j = spark.read \
    .format("json") \
    .option("multiline", "true") \
    .load(f"{base_path}/schemas.json")

schema_j.display()

# COMMAND ----------

column_dict = schema_j.toPandas().to_dict(orient='list')

# COMMAND ----------

# file = "customers"
print((column_dict[file][0]))

# COMMAND ----------

cols = []

for column in (column_dict[file][0]):
    cols.append(f"{column['column_name']} {column['data_type']}")
    # print(column['column_name'], column['data_type'])

# print(cols)

schema = ",".join(cols)

print(schema)

# COMMAND ----------

# schema = "order_id integer,order_date string,order_customer_id string,order_status string"
# schema = "product_id integer,product_category_id integer,product_name string,product_description string,product_price float,product_image string"

# COMMAND ----------


df = spark.read \
    .format("csv") \
    .schema(schema) \
    .load(f"{base_path}/{file}")

df.printSchema()

# COMMAND ----------

# print(df.rdd.getNumPartitions())
# spark.conf.get('spark.sql.files.maxPartitionBytes')
# len(df.inputFiles())
# df.inputFiles()
spark.conf.get("spark.sql.adaptive.enabled")
# spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")
# df_products.cache()
# display(df)


# COMMAND ----------

# df.createOrReplaceTempView("orders_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*) from orders_vw where order_status in ('COMPLETE','CLOSED')

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", 15)


# COMMAND ----------

df__count = df.groupBy("order_item_order_id").count().orderBy("order_item_order_id")

# COMMAND ----------

# print(df__count.rdd.getNumPartitions())
# df__count = df__count.coalesce(15)
# display(df__count)
display(df)

# COMMAND ----------

from pyspark.sql import functions as F

df__count.count()
# result = df_cat_count.agg(F.min("product_category_id"), F.max("product_category_id")).first()

# COMMAND ----------

# print(result)

# COMMAND ----------

from pyspark.sql.functions import col, to_date

df = df.withColumn("order_date", to_date(col("order_date")))

# COMMAND ----------

# df = df.repartition(64)
df = df.repartition(15)

# COMMAND ----------


partition_by = "json_data"
json_data_file = "json_data_file"

df.write \
    .format("json") \
    .mode("overwrite") \
    .partitionBy("order_item_product_id") \
    .save(f"/Volumes/data/raw/partitioned_data/retail_db/json_data_small_size_m/{file}")

# df.write \
#     .format("json") \
#     .mode("overwrite") \
#     .save(f"/Volumes/data/raw/partitioned_data/retail_db/json_data_file/{file}")


# COMMAND ----------

# dbutils.fs.rm("/Volumes/data/raw/partitioned_data/retail_db/orders/", recurse=True)
# dbutils.fs.ls("/Volumes/data/raw/partitioned_data/")
# dbutils.fs.ls("/Volumes/data/raw/partitioned_data/retail_db/")


# COMMAND ----------

# dbutils.fs.cp("/Volumes/data/raw/sales/partitioned_data/", "/Volumes/data/raw/partitioned_data/", recurse=True)

# COMMAND ----------

# dbutils.fs.ls("/Volumes/data/raw/partitioned_data/retail_db/")

# COMMAND ----------

# dbutils.fs.mkdirs("/Volumes/data/raw/dev/tpch")
# dbutils.fs.ls("/Volumes/data/raw/dev/")

# COMMAND ----------

df_orders = spark.read \
    .format("csv") \
    .option("header", True) \
    .load("/Volumes/data/raw/partitioned_data/retail_db/order_items")

# COMMAND ----------

df_orders.createOrReplaceTempView("order_items_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order_items_vw where order_item_order_id = 50023 and order_item_quantity > 1;
# MAGIC -- describe orders_vw;

# COMMAND ----------

