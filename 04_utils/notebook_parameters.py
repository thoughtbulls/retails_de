# Databricks notebook source
dbutils.widgets.text("dept", "", "Department")

# COMMAND ----------

dept = dbutils.widgets.get("dept")
print(dept)

# COMMAND ----------

emp_df = spark.sql(f"select * from sales.bronze.employees")
# emp_df.printSchema()
display(emp_df)
emp_df.filter(f"dept = '{dept}'").display()
# only active employees
emp_df.filter(f"dept = '{dept}' and is_active = 'Y'").display()

# COMMAND ----------

spark.table("sales.bronze.employees").filter(f"dept = '{dept}'").display()

# COMMAND ----------

