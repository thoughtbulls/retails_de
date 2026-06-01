# Databricks notebook source
dbutils.notebook.run("notebook_parameters", 600, 
                     {"dept": "Sales",
                      "is_active": 'Y'
                      
                      })
# emp_df = spark.read.table("emp")

# COMMAND ----------

dbutils.notebook.run("notebook_parameters", 600, 
                     {"dept": "Marketing", "is_active": 'Y'})

# COMMAND ----------

