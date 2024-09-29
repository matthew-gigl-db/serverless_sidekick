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

workspace_host = dbutils.widgets.get("workspace.host")
db_pat = dbutils.secrets.get(scope=dbutils.widgets.get("secret_scope"), key=dbutils.widgets.get("secret_databricks_pat"))
workspace_src_path = dbutils.widgets.get("workspace.file_path") + "/src/"
existing_job_ids = dbutils.widgets.get("existing_job_ids")
existing_pipeline_ids = dbutils.widgets.get("existing_pipeline_ids")

# COMMAND ----------

print(f"""
  workspace_host: {workspace_host}
  db_pat: {db_pat}
  workspace_src_path: {workspace_src_path}
  existing_job_ids: {existing_job_ids}
  existing_pipeline_ids: {existing_pipeline_ids}
""")

# COMMAND ----------

# DBTITLE 1,Create DAB Default Python Config File
import json

# Create dab_init_config json 
# note that this is the default-python specificiation based on https://github.com/databricks/cli/blob/a13d77f8eb29a6c7587509721217a137039f20d6/libs/template/templates/default-python/databricks_template_schema.json#L3
dab_init_config = {
    "project_name": project
    ,"include_notebook": "no"
    ,"include_dlt": "no"
    ,"include_python": "no"
}
dab_init_config = json.dumps(dab_init_config)

# Print dab_init_config as formatted JSON
print(json.dumps(json.loads(dab_init_config), indent=4))

# COMMAND ----------

# DBTITLE 1,Create Temporary Folder in 'Run As' User's Home Directory
from pathlib import Path
from tempfile import TemporaryDirectory

home_dir = str(Path.home())
Dir = TemporaryDirectory(dir=home_dir)
temp_directory = Dir.name

temp_directory

# COMMAND ----------

# DBTITLE 1,Write Config File to the Directory
with open(f"{temp_directory}/dab_init_config.json", "w") as file:
    file.write(dab_init_config)

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(workspace_src_path))

# COMMAND ----------

import dabAssist

# COMMAND ----------

# MAGIC %md
# MAGIC ## Not Run
# MAGIC ***

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ***
