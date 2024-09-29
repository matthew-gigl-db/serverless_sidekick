# Databricks notebook source
# DBTITLE 1,Databricks CLI Parameters
dbutils.widgets.text("workspace.host", "https://e2-demo-field-eng.cloud.databricks.com/", "Workspace URL")
dbutils.widgets.text("secret_scope", spark.sql("select replace(substring_index(current_user(), '@', 1), '.', '-')").collect()[0][0], "Secret Scope")
dbutils.widgets.text("secret_databricks_pat", "databricks_pat", "DB Secret for Databricks PAT")

# COMMAND ----------

# DBTITLE 1,Generate YAML Parameters

dbutils.widgets.text("workflow_name", "", "Workflow Name")
dbutils.widgets.text( "existing_job_ids", "", "Existing Job IDs")
dbutils.widgets.text("existing_pipeline_ids", "", "Existing Pipeline Ids")
dbutils.widgets.text("workspace.file_path", spark.sql("select '/Workspace/Users/' || current_user() || '/serverless_sidekick'").collect()[0][0], "Workspace File Path")


# COMMAND ----------

existing_job_ids = dbutils.widgets.get("existing_job_ids")

# COMMAND ----------

for job_id in existing_job_ids.split(","):
  print(job_id.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Not Run
# MAGIC ***

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ***
