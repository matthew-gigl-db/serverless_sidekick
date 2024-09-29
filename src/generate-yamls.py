# Databricks notebook source
# MAGIC %md
# MAGIC #### Example Inputs: 
# MAGIC - **workflow_name**:  dlt_dropbox_job  
# MAGIC - **existing_job_ids**: 770567817966568  
# MAGIC - **existing_pipeline_ids**: 6d6c88e7-7abb-453c-968d-0f2f37ab4dce, de60c372-2efe-4697-8415-b3076d48b74f  

# COMMAND ----------

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

# DBTITLE 1,Retrieve Parameter Inputs
workspace_url = dbutils.widgets.get("workspace.host")
db_pat = dbutils.secrets.get(scope=dbutils.widgets.get("secret_scope"), key=dbutils.widgets.get("secret_databricks_pat"))
workspace_src_path = dbutils.widgets.get("workspace.file_path") + "/src/"
existing_job_ids = dbutils.widgets.get("existing_job_ids")
existing_pipeline_ids = dbutils.widgets.get("existing_pipeline_ids")
project = dbutils.widgets.get("workflow_name")

# COMMAND ----------

# DBTITLE 1,Print Parameter Inputs
print(f"""
  workspace_url: {workspace_url}
  db_pat: {db_pat}
  workspace_src_path: {workspace_src_path}
  existing_job_ids: {existing_job_ids}
  existing_pipeline_ids: {existing_pipeline_ids}
  workflow_name: {project}
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

# DBTITLE 1,Append Workspace Source to Path Variable
import sys, os
sys.path.append(os.path.abspath(workspace_src_path))

# COMMAND ----------

# DBTITLE 1,Import dabAssist
import dabAssist

# COMMAND ----------

# DBTITLE 1,Initialize the Databricks CLI Class
dc = dabAssist.databricksCli(
  workspace_url = workspace_url
  ,db_pat = db_pat
)
dc

# COMMAND ----------

# DBTITLE 1,Install the Databricks CLI
dc.install()

# COMMAND ----------

# DBTITLE 1,Configure the Databricks CLI
dc.configure().returncode

# COMMAND ----------

# DBTITLE 1,Initialize a dabAssist Class Object
bundle = dabAssist.assetBundle(
  directory = temp_directory
  ,repo_url = ""  # note that repo URL is not used when its not known yet
  ,project = project
  ,cli_path = dc.cli_path
  ,target = "dev"
)

# COMMAND ----------

# DBTITLE 1,Intialize a Databricks Asset Bundle
print(
  bundle.initialize(
    template = "default-python"
    ,config_file = "dab_init_config.json"
  )
)

# COMMAND ----------

# DBTITLE 1,Generate YAMLs for Existing Jobs
existing_job_ids = dbutils.widgets.get("existing_job_ids").split(",")

if len(existing_job_ids) > 0 and existing_job_ids != ['']:
  for i in existing_job_ids:
    print(
      bundle.generate_yaml(
        existing_id = i.strip()
        ,type = "job"
      )
    )

# COMMAND ----------

# DBTITLE 1,Generate YAMLs for Existing Pipelines
existing_pipeline_ids = dbutils.widgets.get("existing_pipeline_ids").split(",")

if len(existing_pipeline_ids) > 0 and existing_pipeline_ids != ['']:
  for i in existing_pipeline_ids:
    print(
      bundle.generate_yaml(
        existing_id = i.strip()
        ,type = "pipeline"
      )
    )

# COMMAND ----------

# DBTITLE 1,List the Databricks Asset Bundle Files Generated
import subprocess

cmd = f"ls -altR {temp_directory}"

result = subprocess.run(cmd, shell=True, capture_output=True)
print(result.stdout.decode("utf-8"))

# COMMAND ----------

# DBTITLE 1,Copy the Generated YAMLs to the Serverless Sidekick's original_yamls Volume
cmd = f"mkdir /Volumes/serverless_sidekick/default/original_yamls/{project}/; cp -rf {temp_directory}/{project}/resources/* /Volumes/serverless_sidekick/default/original_yamls/{project}/"
print(
  f"cmd: {cmd}"
)

result = subprocess.run(cmd, shell=True, capture_output=True)
print(result.stdout.decode("utf-8"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Not Run
# MAGIC ***

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ***
