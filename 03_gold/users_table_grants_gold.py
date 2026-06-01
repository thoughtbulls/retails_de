# Databricks notebook source
GRANTS = "ALTER TABLE retails.gold.dim_products OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.gold.dim_departments OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.gold.dim_categories OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.gold.dim_customers OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.gold.dim_dates OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.gold.fact_orders OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.gold.fact_order_items OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()