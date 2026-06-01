# Databricks notebook source
import os
print(os.popen("hostname -i").read())
print(os.popen("hostname").read())

# COMMAND ----------

# spark.sql("SELECT * FROM systems.runtime.executors").show(truncate=False)
# Query Databricks system tables for cluster node history
spark.sql("SELECT * FROM system.compute.clusters").filter("delete_time IS NULL").display()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     cluster_id, 
# MAGIC     cluster_name, 
# MAGIC     owned_by, 
# MAGIC     create_time, 
# MAGIC     dbr_version,
# MAGIC     worker_count
# MAGIC FROM 
# MAGIC     system.compute.clusters
# MAGIC WHERE 
# MAGIC     delete_time IS NULL
# MAGIC ORDER BY 
# MAGIC     create_time DESC;

# COMMAND ----------

import requests
import json

# Get current workspace domain and token context automatically
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
instance_url = ctx.apiUrl().get()
token = ctx.apiToken().get()

headers = {"Authorization": f"Bearer {token}"}
url = f"{instance_url}/api/2.1/clusters/list"

response = requests.get(url, headers=headers)

if response.status_code == 200:
    clusters = response.json().get("clusters", [])
    print(f"{'Cluster Name':<30} | {'Cluster ID':<25} | {'State':<15}")
    print("-" * 75)
    for c in clusters:
        # Filter strictly for RUNNING, PENDING, or RESIZING states
        if c['state'] in ['RUNNING', 'PENDING', 'RESIZING']:
            print(f"{c['cluster_name']:<30} | {c['cluster_id']:<25} | {c['state']:<15}")
else:
    print(f"Failed to fetch clusters: {response.text}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     usage_metadata.cluster_id,
# MAGIC     COALESCE(custom_tags['DatabricksClusterName'], usage_metadata.cluster_id) as cluster_name,
# MAGIC     SUM(usage_quantity) as total_dbus
# MAGIC FROM 
# MAGIC     system.billing.usage
# MAGIC WHERE 
# MAGIC     usage_date >= current_date() - INTERVAL 30 DAY
# MAGIC     AND usage_metadata.cluster_id IS NOT NULL
# MAGIC GROUP BY 
# MAGIC     1, 2
# MAGIC ORDER BY 
# MAGIC     total_dbus DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from system.compute.clusters;
# MAGIC -- select * from system.lakeflow.job_run_timeline;
# MAGIC select * from system.storage.predictive_optimization_operations_history;

# COMMAND ----------

import socket
print("Driver IP:", socket.gethostbyname(socket.gethostname()))

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import socket

def get_ip():
    return socket.gethostbyname(socket.gethostname())

get_ip_udf = udf(get_ip, StringType())

spark.range(100).withColumn("executor_ip", get_ip_udf()).distinct().show()

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import socket

def get_ip():
    return socket.gethostbyname(socket.gethostname())

get_ip_udf = udf(get_ip, StringType())

ips = spark.range(1000).withColumn("executor_ip", get_ip_udf()).select("executor_ip").distinct()
ips.show(truncate=False)

# COMMAND ----------

def get_all_ips():
    import os
    return os.popen("hostname -I").read()

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

get_all_ips_udf = udf(get_all_ips, StringType())

spark.range(10).withColumn("ips", get_all_ips_udf()).show(truncate=False)

# COMMAND ----------

df = spark.range(1000).withColumn("executor_ip", get_ip_udf())

df.groupBy("executor_ip").count().show()

# COMMAND ----------

