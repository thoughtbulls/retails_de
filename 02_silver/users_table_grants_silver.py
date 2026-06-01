# Databricks notebook source
GRANTS = "ALTER TABLE retails.silver.cdc_rejected_events OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.departments_cleaned OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.departments_quarantine OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.categories_cleaned OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.categories_quarantine OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.products_cleaned OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.products_quarantine OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.customers_cleaned OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.customers_quarantine OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.orders_cleaned OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.orders_quarantine OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.order_items_cleaned OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()

# COMMAND ----------

GRANTS = "ALTER TABLE retails.silver.order_items_quarantine OWNER TO `dp-sales-engineers`"
spark.sql(GRANTS).display()