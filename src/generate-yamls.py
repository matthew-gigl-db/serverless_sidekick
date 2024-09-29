# Databricks notebook source
dbutils.widgets.text("existing_job_ids", "", "Existing Job IDs")

# COMMAND ----------

existing_job_ids = dbutils.widgets.get("existing_job_ids")

# COMMAND ----------

for job_id in existing_job_ids.split(","):
  print(job_id.strip())
