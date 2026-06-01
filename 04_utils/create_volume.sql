-- Databricks notebook source
show catalogs;

-- COMMAND ----------

show schemas in samples;

-- COMMAND ----------

show tables in samples.nyctaxi;

-- COMMAND ----------

select * from samples.nyctaxi.trips;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("samples.nyctaxi.trips")
-- MAGIC df.display()
-- MAGIC df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.groupBy("pickup_zip").count().orderBy("count", ascending=False).show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").option("overwriteSchema", "true").partitionBy("pickup_zip").saveAsTable("nyctaxi.bronze.trips")

-- COMMAND ----------

describe table extended nyctaxi.bronze.trips;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("nyctaxi.bronze.trips")
-- MAGIC df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.filter(df.dropoff_zip == 10020).display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.filter((df.pickup_zip == 10014) & (df.dropoff_zip == 10019)).display()

-- COMMAND ----------

show volumes in nyctaxi.bronze;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.text("/Volumes/nyctaxi/bronze/taxi/retail_db/customers")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC customers = spark.read.csv("/Volumes/nyctaxi/bronze/taxi/retail_db/customers")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(customers)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json
-- MAGIC
-- MAGIC # schema_dict = spark.read.json("/Volumes/nyctaxi/bronze/taxi/retail_db/schemas.json").first().asDict()
-- MAGIC schema_dict = spark.read \
-- MAGIC     .option("multiline", "true") \
-- MAGIC     .json("/Volumes/nyctaxi/bronze/taxi/retail_db/schemas.json") \
-- MAGIC     .first() \
-- MAGIC     .asDict()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(schema_dict.keys())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for key in schema_dict.keys():
-- MAGIC     if key == "customers":
-- MAGIC         customer_schema = schema_dict.get(key)
-- MAGIC         print(customer_schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC def get_type(t):
-- MAGIC     return {
-- MAGIC         "integer": IntegerType(),
-- MAGIC         "string": StringType(),
-- MAGIC         "float": FloatType(),
-- MAGIC         "timestamp": TimestampType()
-- MAGIC     }.get(t.lower() if t else "", StringType())
-- MAGIC
-- MAGIC # sort columns
-- MAGIC cols = sorted(customer_schema, key=lambda x: x["column_position"])
-- MAGIC # print(cols)
-- MAGIC
-- MAGIC # build schema
-- MAGIC schema = StructType([
-- MAGIC     StructField(col["column_name"], get_type(col["data_type"]), True)
-- MAGIC     for col in cols
-- MAGIC ])
-- MAGIC
-- MAGIC # print(schema)
-- MAGIC
-- MAGIC # for col in cols:
-- MAGIC #     s_type = StructType([
-- MAGIC #         StructField(col["column_name"], get_type(col["data_type"]), True)
-- MAGIC #         ])
-- MAGIC #     print(s_type)
-- MAGIC
-- MAGIC types = [
-- MAGIC         StructField(col["column_name"], get_type(col["data_type"]), True)
-- MAGIC         for col in cols
-- MAGIC         ]
-- MAGIC schema = StructType(types)
-- MAGIC print(schema)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read \
-- MAGIC     .option("header", "false") \
-- MAGIC     .schema(schema) \
-- MAGIC     .csv("/Volumes/nyctaxi/bronze/taxi/retail_db/customers/")
-- MAGIC
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC {
-- MAGIC         "integer": IntegerType(),
-- MAGIC         "string": StringType(),
-- MAGIC         "float": FloatType(),
-- MAGIC         "timestamp": TimestampType()
-- MAGIC     }.get("string")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC numbers = [1, 2, 3]
-- MAGIC squares = [x*x for x in numbers]
-- MAGIC print(list(squares))

-- COMMAND ----------

